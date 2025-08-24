#!/usr/bin/env Rscript
# ---------------------------------------------------------------
#  Scrape tweets for a set of handles and upsert into Supabase,
#  plus snapshot follower counts to user_followers.
#  Now includes:
#   • expanded fields (hashtags/cashtags/threading flags)
#   • conversation_id "fix" rules per handle
#   • collapsed threads table (twitter_threads)
# ---------------------------------------------------------------

## 0 – packages --------------------------------------------------
need <- c("reticulate","jsonlite","purrr","dplyr","lubridate",
          "DBI","RPostgres","tibble","stringr","readr","tidyr")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

`%||%` <- function(x, y) if (is.null(x)) y else x

## 1 – Python env & twscrape ------------------------------------
venv <- Sys.getenv("PY_VENV_PATH", ".venv")
if (!dir.exists(venv)) {
  reticulate::virtualenv_create(venv, python = NULL)
  reticulate::virtualenv_install(venv, "twscrape")
}
reticulate::use_virtualenv(venv, required = TRUE)

twscrape <- import("twscrape", convert = FALSE)
asyncio  <- import("asyncio",  convert = FALSE)
api      <- twscrape$API()

## 1a – helpers to avoid 32-bit int issues ----------------------
reticulate::py_run_string("
from collections.abc import Mapping, Sequence
INT32_MAX =  2_147_483_647
INT32_MIN = -2_147_483_648

def _fix_ints(obj):
    if isinstance(obj, int) and not (INT32_MIN <= obj <= INT32_MAX):
        return str(obj)  # keep as str if it won't fit
    if isinstance(obj, Mapping):
        return {k: _fix_ints(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_fix_ints(v) for v in obj]
    return obj

def list_of_safe_dicts(tweets):
    return [_fix_ints(t.dict()) for t in tweets]
")

# ← NEW: import the module where the functions above were defined
pymain      <- reticulate::import("__main__", convert = FALSE)
pybuiltins  <- reticulate::import_builtins()
py_none     <- pybuiltins$None

as_chr <- function(x) if (!identical(x, py_none)) reticulate::py_str(x) else NA_character_
as_num <- function(x) if (!identical(x, py_none)) suppressWarnings(as.numeric(reticulate::py_str(x))) else NA_real_

## 2 – Add account (needs cookies) ------------------------------
cookie_json <- Sys.getenv("TW_COOKIES_JSON")
if (cookie_json == "") stop("TW_COOKIES_JSON env var not set")

cookies_list <- jsonlite::fromJSON(cookie_json)
cookies_str  <- paste(paste0(cookies_list$name, "=", cookies_list$value), collapse = "; ")
asyncio$run(api$pool$add_account("x","x","x","x", cookies=cookies_str))

message(sprintf("✅ %d cookies loaded, total chars = %d",
                nrow(cookies_list), nchar(cookies_str)))

## 3 – scrape ----------------------------------------------------
handles <- trimws(strsplit(
  Sys.getenv("TW_HANDLES","aoTheComputer,ar_io_network,samecwilliams"), ","
)[[1]])
message("✅ Handles: ", paste(handles, collapse = ", "))

# --- turn one Tweet (as an R list) into a tidy row --------------
tweet_to_row <- function(tw, user) {
  # tw is an R list (already py_to_r()-converted)
  tags   <- tw$hashtags
  ctags  <- tw$cashtags
  in_usr <- tw$inReplyToUser
  user_obj <- tw$user

  hashtags <- if (!is.null(tags)  && length(tags))  stringr::str_c(unlist(tags),  collapse = ",") else NA_character_
  cashtags <- if (!is.null(ctags) && length(ctags)) stringr::str_c(unlist(ctags), collapse = ",") else NA_character_

  parent_user_username <- if (!is.null(in_usr) && !is.null(in_usr$username)) in_usr$username else NA_character_

  id_str  <- as.character(tw$id %||% NA)
  uid_str <- if (!is.null(user_obj) && !is.null(user_obj$id)) as.character(user_obj$id) else NA_character_

  created <- tw$date %||% NA_character_
  text    <- tw$rawContent %||% NA_character_

  like    <- suppressWarnings(as.numeric(tw$likeCount    %||% NA))
  retweet <- suppressWarnings(as.numeric(tw$retweetCount %||% NA))
  reply   <- suppressWarnings(as.numeric(tw$replyCount   %||% NA))
  quote   <- suppressWarnings(as.numeric(tw$quoteCount   %||% NA))
  views   <- suppressWarnings(as.numeric(tw$viewCount    %||% NA))

  tibble::tibble(
    username = user,
    tweet_id = id_str,
    tweet_url = sprintf("https://twitter.com/%s/status/%s", user, id_str),
    user_id  = uid_str,
    created  = as.character(created),
    text     = text,
    like     = like,
    retweet  = retweet,
    reply    = reply,
    quote    = quote,
    views    = views,
    hashtags = hashtags,
    cashtags = cashtags,
    conversation_id      = as.character(tw$conversationId %||% NA),
    parent_tweet_id      = as.character(tw$inReplyToTweetId %||% NA),
    parent_user_username = parent_user_username,
    is_reply   = !is.null(tw$inReplyToTweetId),
    is_quote   = !is.null(tw$quotedTweet),
    is_retweet = !is.null(tw$retweetedTweet)
  )
}

# scrape one handle → python list → safe dicts → R lists → rows --
scrape_one <- function(user, limit = 100L) {
  tryCatch({
    info  <- asyncio$run(api$user_by_login(user))
    me_id <- as_chr(info$id)

    # followers snapshot (global <<- append)
    followers_df <<- dplyr::bind_rows(
      followers_df,
      tibble::tibble(
        username        = user,
        user_id         = me_id,
        followers_count = as_num(info$followersCount),
        snapshot_time   = Sys.time()
      )
    )

    tweets <- asyncio$run(
      twscrape$gather(api$user_tweets_and_replies(info$id, limit=as.integer(limit)))
    )
    n_tw <- reticulate::py_len(tweets)
    message(sprintf("✅ %s → %d tweets", user, n_tw))
    if (n_tw == 0) return(NULL)

    # convert to safe dicts (avoid 32-bit int trouble) then to R
    safe_py <- pymain$list_of_safe_dicts(tweets)
    safe_r  <- reticulate::py_to_r(safe_py)

    purrr::map_dfr(safe_r, tweet_to_row, user = user)
  }, error = function(e) {
    message(sprintf("❌ %s → %s", user, conditionMessage(e)))
    NULL
  })
}

# globals
followers_df <- tibble::tibble(username=character(), user_id=character(),
                               followers_count=numeric(), snapshot_time=as.POSIXct(character()))

tidy_tbl <- purrr::map_dfr(handles, scrape_one)
if (nrow(tidy_tbl) == 0) stop("No tweets scraped — aborting.")

# Sort most recent first (helps the fix pass)
tidy_tbl <- tidy_tbl |> dplyr::arrange(username, dplyr::desc(created))

# optional sanity flag for odd RT text patterns
tidy_tbl <- tidy_tbl |> dplyr::mutate(is_rt_text = stringr::str_detect(text %||% "", "^RT @"))

## 3b – fix conversation_id by handle (Regla 1/2) ----------------
fix_conversation_id <- function(df) {
  n   <- nrow(df); if (!n) return(df)
  cid <- df$conversation_id
  iq  <- df$is_quote
  idx <- which(iq & (seq_len(n) < n))  # every is_quote except last row

  for (i in idx) {
    # Regla 2: if i+1 and i+2 share same cid, adopt cid[i+1] into row i
    if (i + 2 <= n && !is.na(cid[i+1]) && cid[i + 1] == cid[i + 2]) {
      cid[i] <- cid[i + 1]
    }
    # Regla 1: row below adopts (possibly corrected) cid[i]
    if (i + 1 <= n) cid[i + 1] <- cid[i]
  }

  df$conversation_id <- cid
  df
}

tidy_fix <- tidy_tbl |>
  dplyr::group_by(username) |>
  dplyr::group_modify(~ fix_conversation_id(.x)) |>
  dplyr::ungroup()

## 3c – collapse by conversation_id ------------------------------
collapsed_tbl <- tidy_fix |>
  dplyr::group_by(conversation_id) |>
  dplyr::summarise(
    username   = dplyr::first(username),
    created    = suppressWarnings(max(lubridate::as_datetime(created), na.rm = TRUE)),
    text       = stringr::str_c(text, collapse = "\n\n"),
    like       = sum(like,    na.rm = TRUE),
    retweet    = sum(retweet, na.rm = TRUE),
    reply      = sum(reply,   na.rm = TRUE),
    quote      = sum(quote,   na.rm = TRUE),
    views      = sum(views,   na.rm = TRUE),
    n_tweets   = dplyr::n(),
    .groups = "drop"
  ) |>
  dplyr::mutate(created = as.character(created))

## 3d – compute cleaned engagement_rate on tidy tweets -----------
tidy_fix <- tidy_fix |>
  dplyr::mutate(
    high_er_flag = !is.na(views) & (reply + retweet + like + quote) > views,
    er_calc = dplyr::if_else(is.na(views) | views == 0, NA_real_,
                             100 * (reply + retweet + like + quote) / views),
    suspicious_retweet = !is.na(er_calc) & (er_calc > 50) & is_retweet,
    engagement_rate = dplyr::if_else(high_er_flag | suspicious_retweet, NA_real_, er_calc)
  ) |>
  dplyr::select(-er_calc)

## 4 – Supabase connection --------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (supa_pwd == "") stop("Supabase password env var not set")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = Sys.getenv("SUPABASE_DB", "postgres"),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

## 4a – twitter_raw (expanded schema + migration) ----------------
DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS twitter_raw (
    tweet_id text PRIMARY KEY,
    tweet_url text,
    username text,
    user_id  text,
    created  timestamptz,
    text     text,
    reply    integer,
    retweet  integer,
    like     integer,
    quote    integer,
    views    bigint,
    hashtags text,
    cashtags text,
    conversation_id text,
    parent_tweet_id text,
    parent_user_username text,
    is_reply boolean,
    is_quote boolean,
    is_retweet boolean,
    is_rt_text boolean,
    engagement_rate numeric
  );
")

# add any missing columns on older installs
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS tweet_url text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS created timestamptz;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS reply integer;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS retweet integer;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS like integer;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS quote integer;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS views bigint;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS hashtags text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS cashtags text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS conversation_id text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS parent_tweet_id text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS parent_user_username text;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS is_reply boolean;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS is_quote boolean;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS is_retweet boolean;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS is_rt_text boolean;") )
invisible( DBI::dbExecute(con, "ALTER TABLE twitter_raw ADD COLUMN IF NOT EXISTS engagement_rate numeric;") )

# stage → upsert
DBI::dbWriteTable(con, "tmp_twitter_raw", tidy_fix, temporary = TRUE, overwrite = TRUE)

DBI::dbExecute(con, "
  WITH dedup AS (
    SELECT DISTINCT ON (tweet_id) *
    FROM tmp_twitter_raw
    ORDER BY tweet_id, created DESC NULLS LAST
  )
  INSERT INTO twitter_raw AS t
    (tweet_id, tweet_url, username, user_id, created, text,
     reply, retweet, like, quote, views,
     hashtags, cashtags, conversation_id, parent_tweet_id, parent_user_username,
     is_reply, is_quote, is_retweet, is_rt_text, engagement_rate)
  SELECT tweet_id, tweet_url, username, user_id, created::timestamptz, text,
         reply, retweet, like, quote, views,
         hashtags, cashtags, conversation_id, parent_tweet_id, parent_user_username,
         is_reply, is_quote, is_retweet, is_rt_text, engagement_rate
  FROM dedup
  ON CONFLICT (tweet_id) DO UPDATE SET
    tweet_url   = EXCLUDED.tweet_url,
    text        = EXCLUDED.text,
    reply       = EXCLUDED.reply,
    retweet     = EXCLUDED.retweet,
    like        = EXCLUDED.like,
    quote       = EXCLUDED.quote,
    views       = EXCLUDED.views,
    hashtags    = EXCLUDED.hashtags,
    cashtags    = EXCLUDED.cashtags,
    conversation_id = EXCLUDED.conversation_id,
    parent_tweet_id = EXCLUDED.parent_tweet_id,
    parent_user_username = EXCLUDED.parent_user_username,
    is_reply    = EXCLUDED.is_reply,
    is_quote    = EXCLUDED.is_quote,
    is_retweet  = EXCLUDED.is_retweet,
    is_rt_text  = EXCLUDED.is_rt_text,
    engagement_rate = EXCLUDED.engagement_rate;
")

DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_raw;")

## 4b – twitter_threads (collapsed conversations) ----------------
DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS twitter_threads (
    conversation_id text PRIMARY KEY,
    username   text,
    created    timestamptz,
    text       text,
    like       bigint,
    retweet    bigint,
    reply      bigint,
    quote      bigint,
    views      bigint,
    n_tweets   integer
  );
")

DBI::dbWriteTable(con, "tmp_twitter_threads", collapsed_tbl, temporary = TRUE, overwrite = TRUE)

DBI::dbExecute(con, "
  INSERT INTO twitter_threads AS th
    (conversation_id, username, created, text, like, retweet, reply, quote, views, n_tweets)
  SELECT conversation_id, username, created::timestamptz, text, like, retweet, reply, quote, views, n_tweets
  FROM tmp_twitter_threads
  ON CONFLICT (conversation_id) DO UPDATE SET
    username = EXCLUDED.username,
    created  = EXCLUDED.created,
    text     = EXCLUDED.text,
    like     = EXCLUDED.like,
    retweet  = EXCLUDED.retweet,
    reply    = EXCLUDED.reply,
    quote    = EXCLUDED.quote,
    views    = EXCLUDED.views,
    n_tweets = EXCLUDED.n_tweets;
")

DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_threads;")

## 4c – user_followers -------------------------------------------
DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS user_followers (
    user_id         text,
    username        text,
    followers_count bigint,
    snapshot_time   timestamptz DEFAULT now(),
    PRIMARY KEY (user_id, snapshot_time)
  );
")
DBI::dbWriteTable(con, "user_followers", followers_df, append = TRUE, row.names = FALSE)

## 5 – wrap up ---------------------------------------------------
DBI::dbDisconnect(con)
message("✅ Tweets (raw + threads) & follower counts upserted at ", Sys.time())

## 5 – wrap up ---------------------------------------------------
DBI::dbDisconnect(con)
message("✅ Tweets (raw + threads) & follower counts upserted at ", Sys.time())

