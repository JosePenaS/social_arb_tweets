###############################################################################
# twitter_ingest.R
#
# Robust X/Twitter ingest script with rolling lookback window.
#
# Main change:
#   - Instead of scraping only fixed 9:00/16:30 NY windows, this script now
#     scrapes a rolling lookback window by default.
#   - Default: last 36 hours.
#   - Override with env var X_LOOKBACK_HOURS.
#   - You can still manually override the exact window with X_START_TIME and
#     X_END_TIME in UTC ISO format.
#
# Outputs:
#   public.twitter_raw_v2
#   public.twitter_collapse_full
###############################################################################

suppressPackageStartupMessages({
  library(httr2)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(DBI)
  library(RPostgres)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

###############################################################################
# 0 · ENV VARS ----------------------------------------------------------------
###############################################################################

BEARER <- Sys.getenv("X_BEARER")
if (BEARER == "") stop("Missing env var: X_BEARER")

SUPABASE_PORT <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
SUPABASE_DB   <- Sys.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER <- Sys.getenv("SUPABASE_USER")
SUPABASE_PWD  <- Sys.getenv("SUPABASE_PWD")
SUPABASE_POOLER_HOST <- Sys.getenv("SUPABASE_POOLER_HOST")

if (!nzchar(SUPABASE_POOLER_HOST)) stop("Missing env var: SUPABASE_POOLER_HOST")
if (!nzchar(SUPABASE_USER)) stop("Missing env var: SUPABASE_USER")
if (!nzchar(SUPABASE_PWD))  stop("Missing env var: SUPABASE_PWD")

# Handles list (comma-separated) OR fallback list
handles <- trimws(strsplit(Sys.getenv(
  "TW_HANDLES",
  "ChrisCamillo,NolanAntonucci,ConsensusGurus,jordan_mclain,tickerplus,davehanson,AstutexAi,SingularityRes"
), ",")[[1]])

handles <- handles[nzchar(handles)]
if (length(handles) == 0) stop("No handles provided")

message("✅ Handles: ", paste(handles, collapse = ", "))

###############################################################################
# 1 · Time window --------------------------------------------------------------
###############################################################################

# Manual override:
#   X_START_TIME=2026-06-17T00:00:00Z
#   X_END_TIME=2026-06-18T00:00:00Z
#
# Default:
#   rolling lookback window, controlled by X_LOOKBACK_HOURS.
#   Default is 36 hours.
#
# This is safer than anchored 9:00/16:30 windows because scheduled GitHub runs
# can be delayed or skipped, and duplicate tweets are safe because we upsert by
# tweet_id.

end_time   <- Sys.getenv("X_END_TIME")
start_time <- Sys.getenv("X_START_TIME")

compute_rolling_window <- function() {
  lookback_hours <- suppressWarnings(as.numeric(Sys.getenv("X_LOOKBACK_HOURS", "36")))
  if (is.na(lookback_hours) || lookback_hours <= 0) lookback_hours <- 36

  now_utc <- lubridate::with_tz(Sys.time(), "UTC")
  start_utc <- now_utc - lubridate::hours(lookback_hours)

  list(
    start_time = format(start_utc, "%Y-%m-%dT%H:%M:%SZ"),
    end_time   = format(now_utc,   "%Y-%m-%dT%H:%M:%SZ"),
    start_utc  = start_utc,
    end_utc    = now_utc,
    lookback_hours = lookback_hours
  )
}

if (!nzchar(end_time) || !nzchar(start_time)) {
  w <- compute_rolling_window()
  start_time <- w$start_time
  end_time   <- w$end_time

  message(
    "✅ Rolling window UTC: ",
    start_time,
    " → ",
    end_time,
    " | lookback_hours=",
    w$lookback_hours
  )

  message(
    "✅ Rolling window New York: ",
    lubridate::with_tz(w$start_utc, "America/New_York"),
    " → ",
    lubridate::with_tz(w$end_utc, "America/New_York")
  )

} else {
  message("✅ Window override (env): ", start_time, " → ", end_time)
}

message("✅ Window UTC: ", start_time, " → ", end_time)

###############################################################################
# 2 · Lookup user IDs ----------------------------------------------------------
###############################################################################

x_lookup_user_ids <- function(usernames, bearer) {
  stopifnot(length(usernames) >= 1)

  req <- request("https://api.x.com/2/users/by") |>
    req_headers(Authorization = paste("Bearer", bearer)) |>
    req_url_query(usernames = paste(usernames, collapse = ",")) |>
    req_error(is_error = function(resp) FALSE)

  resp <- req_perform(req)

  if (resp_status(resp) != 200) {
    cat("STATUS:", resp_status(resp), "\n")
    cat(resp_body_string(resp), "\n")
    stop("User lookup failed.")
  }

  json <- resp_body_json(resp)

  if (is.null(json$data)) {
    return(tibble(username = character(), user_id = character()))
  }

  tibble(
    username = vapply(json$data, \(x) x$username, ""),
    user_id  = vapply(json$data, \(x) x$id, "")
  )
}

lookup_user_ids_with_retry <- function(handles, bearer, max_tries = 5) {
  last_error <- NULL

  for (attempt in seq_len(max_tries)) {
    message("🔎 Looking up X user IDs. Attempt ", attempt, " of ", max_tries)

    out <- tryCatch(
      x_lookup_user_ids(handles, bearer),
      error = function(e) {
        last_error <<- e
        NULL
      }
    )

    if (!is.null(out) && nrow(out) > 0) {
      message("✅ User lookup succeeded.")
      return(out)
    }

    wait_seconds <- min(90, 10 * attempt)
    message("⚠️ User lookup failed. Waiting ", wait_seconds, " seconds before retrying...")
    Sys.sleep(wait_seconds)
  }

  last_msg <- if (!is.null(last_error)) conditionMessage(last_error) else "unknown error"
  stop("User lookup failed after ", max_tries, " attempts. Last error: ", last_msg)
}

accounts <- lookup_user_ids_with_retry(handles, BEARER)

if (nrow(accounts) == 0) {
  stop("No accounts returned from lookup")
}

message("✅ Accounts returned: ", nrow(accounts))

###############################################################################
# 3 · Fetch tweets in window ---------------------------------------------------
###############################################################################

empty_posts_tbl <- function() {
  tibble(
    id = character(),
    created_at = as.POSIXct(character(), tz = "UTC"),
    text = character(),
    conversation_id = character(),
    referenced_tweets = list(),
    is_reply = logical(),
    is_quote = logical(),
    is_retweet = logical(),
    is_rt_text = logical(),
    username = character(),
    user_id = character()
  )
}

x_get_user_tweets_window <- function(user_id,
                                     bearer,
                                     start_time,
                                     end_time,
                                     max_pages = 50,
                                     sleep_s = 1,
                                     include_retweets = TRUE) {

  base <- paste0("https://api.x.com/2/users/", user_id, "/tweets")

  query <- list(
    start_time     = start_time,
    end_time       = end_time,
    max_results    = "100",
    `tweet.fields` = "id,created_at,text,conversation_id,referenced_tweets",
    exclude        = if (include_retweets) NULL else "retweets"
  )

  out <- list()
  token <- NULL

  for (i in seq_len(max_pages)) {
    req <- request(base) |>
      req_headers(Authorization = paste("Bearer", bearer)) |>
      req_url_query(!!!query) |>
      req_error(is_error = function(resp) FALSE)

    if (!is.null(token)) {
      req <- req |> req_url_query(pagination_token = token)
    }

    resp <- req_perform(req)

    if (resp_status(resp) != 200) {
      msg <- resp_body_string(resp)
      stop("Tweet fetch failed: ", resp_status(resp), " ", msg)
    }

    json <- resp_body_json(resp)
    data <- json$data %||% list()

    if (length(data) == 0) break

    out <- c(out, data)

    token <- json$meta$next_token %||% NULL
    if (is.null(token)) break

    Sys.sleep(sleep_s)
  }

  if (length(out) == 0) {
    return(tibble(
      id = character(),
      created_at = as.POSIXct(character(), tz = "UTC"),
      text = character(),
      conversation_id = character(),
      referenced_tweets = list()
    ))
  }

  tibble(
    id = vapply(out, \(x) x$id, ""),
    created_at = ymd_hms(vapply(out, \(x) x$created_at %||% NA_character_, ""), tz = "UTC"),
    text = vapply(out, \(x) x$text %||% "", ""),
    conversation_id = vapply(out, \(x) x$conversation_id %||% NA_character_, ""),
    referenced_tweets = lapply(out, \(x) x$referenced_tweets %||% list())
  )
}

infer_flags <- function(refs) {
  if (length(refs) == 0) {
    return(list(is_reply = FALSE, is_quote = FALSE, is_retweet = FALSE))
  }

  types <- vapply(refs, \(r) r$type %||% "", "")

  list(
    is_reply   = any(types == "replied_to"),
    is_quote   = any(types == "quoted"),
    is_retweet = any(types == "retweeted")
  )
}

posts_all <- pmap_dfr(
  list(accounts$user_id, accounts$username),
  function(uid, uname) {

    message("📥 Fetching tweets for @", uname, " (", uid, ")")

    res <- tryCatch(
      x_get_user_tweets_window(
        user_id = uid,
        bearer  = BEARER,
        start_time = start_time,
        end_time   = end_time,
        max_pages  = 50,
        sleep_s    = 1,
        include_retweets = TRUE
      ),
      error = function(e) {
        message("❌ Failed for ", uname, " (", uid, "): ", conditionMessage(e))
        return(tibble(
          id = character(),
          created_at = as.POSIXct(character(), tz = "UTC"),
          text = character(),
          conversation_id = character(),
          referenced_tweets = list()
        ))
      }
    )

    message("   rows returned for @", uname, ": ", nrow(res))

    if (nrow(res) == 0) {
      return(empty_posts_tbl())
    }

    res2 <- res %>%
      rowwise() %>%
      mutate(
        tmp = list(infer_flags(referenced_tweets)),
        is_reply   = tmp$is_reply,
        is_quote   = tmp$is_quote,
        is_retweet = tmp$is_retweet
      ) %>%
      ungroup() %>%
      mutate(
        is_rt_text = str_detect(text %||% "", "^RT @"),
        username   = uname,
        user_id    = uid
      ) %>%
      select(-tmp)

    if (nrow(res2) == 0) {
      empty_posts_tbl()
    } else {
      res2
    }
  }
)

posts_all <- posts_all %>%
  arrange(desc(created_at))

message("✅ Total fetched rows before distinct: ", nrow(posts_all))

if (nrow(posts_all) == 0) {
  message("✅ No tweets fetched in this rolling window. Nothing to upsert.")
  quit(save = "no", status = 0)
}

###############################################################################
# 4 · Prepare tweets_db --------------------------------------------------------
###############################################################################

tweets_db <- posts_all %>%
  transmute(
    tweet_id         = as.character(id),
    created_at       = as.POSIXct(created_at, tz = "UTC"),
    username         = as.character(username),
    user_id          = as.character(user_id),
    text             = as.character(text),
    conversation_id  = as.character(conversation_id),
    is_reply         = as.logical(is_reply),
    is_quote         = as.logical(is_quote),
    is_retweet       = as.logical(is_retweet),
    is_rt_text       = as.logical(is_rt_text)
  ) %>%
  filter(!is.na(tweet_id), nzchar(tweet_id)) %>%
  distinct(tweet_id, .keep_all = TRUE)

message("✅ Distinct tweets_db rows: ", nrow(tweets_db))

if (nrow(tweets_db) == 0) {
  message("✅ No distinct tweet rows after cleaning. Nothing to upsert.")
  quit(save = "no", status = 0)
}

message("✅ Tweets by username:")
print(tweets_db %>% count(username, sort = TRUE))

###############################################################################
# 5 · Fix conversation_id ------------------------------------------------------
###############################################################################

fix_conversation_id <- function(df) {
  n <- nrow(df)
  if (!n) return(df)

  cid <- df$conversation_id
  iq  <- df$is_quote
  idx <- which(iq & (seq_len(n) < n))

  for (i in idx) {
    if (i + 2 <= n && !is.na(cid[i + 1]) && cid[i + 1] == cid[i + 2]) {
      cid[i] <- cid[i + 1]
    }

    if (i + 1 <= n) {
      cid[i + 1] <- cid[i]
    }
  }

  df$conversation_id <- cid
  df
}

tidy_fix <- tweets_db %>%
  group_by(username) %>%
  group_modify(~ fix_conversation_id(.x)) %>%
  ungroup()

###############################################################################
# 6 · Collapse conversations ---------------------------------------------------
###############################################################################

collapsed_tbl <- tidy_fix %>%
  filter(!is.na(conversation_id), nzchar(conversation_id)) %>%
  group_by(conversation_id) %>%
  summarise(
    username = first(username),
    created  = max(created_at, na.rm = TRUE),
    text     = str_c(text, collapse = "\n\n"),
    n_tweets = n(),
    .groups  = "drop"
  )

collapse_db <- collapsed_tbl %>%
  transmute(
    collapse_id     = paste0(conversation_id, ":", username),
    conversation_id = as.character(conversation_id),
    username        = as.character(username),
    created_at      = as.POSIXct(created, tz = "UTC"),
    text            = as.character(text),
    n_tweets        = as.integer(n_tweets)
  ) %>%
  distinct(collapse_id, .keep_all = TRUE)

message("✅ Collapsed conversation rows: ", nrow(collapse_db))

###############################################################################
# 7 · Supabase connection ------------------------------------------------------
###############################################################################

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = SUPABASE_POOLER_HOST,
  port     = SUPABASE_PORT,
  dbname   = SUPABASE_DB,
  user     = SUPABASE_USER,
  password = SUPABASE_PWD,
  sslmode  = "require"
)

on.exit(DBI::dbDisconnect(con), add = TRUE)

print(DBI::dbGetQuery(con, "select '✅ connected' as status, now();"))

###############################################################################
# 8 · Upsert twitter_raw_v2 ----------------------------------------------------
###############################################################################

DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS public.twitter_raw_v2 (
    tweet_id         text PRIMARY KEY,
    created_at       timestamptz,
    username         text,
    user_id          text,
    text             text,
    conversation_id  text,
    is_reply         boolean,
    is_quote         boolean,
    is_retweet       boolean,
    is_rt_text       boolean
  );
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS user_id text;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS conversation_id text;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS is_reply boolean;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS is_quote boolean;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS is_retweet boolean;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_raw_v2
  ADD COLUMN IF NOT EXISTS is_rt_text boolean;
")

DBI::dbWriteTable(
  con,
  "tmp_twitter_raw_v2",
  tweets_db,
  temporary = TRUE,
  overwrite = TRUE
)

DBI::dbExecute(con, "
  INSERT INTO public.twitter_raw_v2 AS t
    (tweet_id, created_at, username, user_id, text, conversation_id, is_reply, is_quote, is_retweet, is_rt_text)
  SELECT
    tweet_id,
    created_at::timestamptz,
    username,
    user_id,
    text,
    conversation_id,
    is_reply,
    is_quote,
    is_retweet,
    is_rt_text
  FROM tmp_twitter_raw_v2
  ON CONFLICT (tweet_id) DO UPDATE
  SET
    created_at      = EXCLUDED.created_at,
    username        = EXCLUDED.username,
    user_id         = EXCLUDED.user_id,
    text            = EXCLUDED.text,
    conversation_id = EXCLUDED.conversation_id,
    is_reply        = EXCLUDED.is_reply,
    is_quote        = EXCLUDED.is_quote,
    is_retweet      = EXCLUDED.is_retweet,
    is_rt_text      = EXCLUDED.is_rt_text;
")

DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_raw_v2;")

message("✅ Upserted twitter_raw_v2 rows: ", nrow(tweets_db))

###############################################################################
# 9 · Upsert twitter_collapse_full --------------------------------------------
###############################################################################

DBI::dbExecute(con, "
  CREATE TABLE IF NOT EXISTS public.twitter_collapse_full (
    collapse_id     text PRIMARY KEY,
    conversation_id text,
    username        text,
    created_at      timestamptz,
    text            text,
    n_tweets        integer
  );
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_collapse_full
  ADD COLUMN IF NOT EXISTS conversation_id text;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_collapse_full
  ADD COLUMN IF NOT EXISTS username text;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_collapse_full
  ADD COLUMN IF NOT EXISTS created_at timestamptz;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_collapse_full
  ADD COLUMN IF NOT EXISTS text text;
")

DBI::dbExecute(con, "
  ALTER TABLE public.twitter_collapse_full
  ADD COLUMN IF NOT EXISTS n_tweets integer;
")

if (nrow(collapse_db) > 0) {
  DBI::dbWriteTable(
    con,
    "tmp_twitter_collapse_full",
    collapse_db,
    temporary = TRUE,
    overwrite = TRUE
  )

  DBI::dbExecute(con, "
    INSERT INTO public.twitter_collapse_full AS t
      (collapse_id, conversation_id, username, created_at, text, n_tweets)
    SELECT
      collapse_id,
      conversation_id,
      username,
      created_at::timestamptz,
      text,
      n_tweets
    FROM tmp_twitter_collapse_full
    ON CONFLICT (collapse_id) DO UPDATE
    SET
      conversation_id = EXCLUDED.conversation_id,
      username        = EXCLUDED.username,
      created_at      = EXCLUDED.created_at,
      text            = EXCLUDED.text,
      n_tweets        = EXCLUDED.n_tweets;
  ")

  DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_collapse_full;")

  message("✅ Upserted twitter_collapse_full rows: ", nrow(collapse_db))

} else {
  message("✅ No collapsed conversations to upsert.")
}

###############################################################################
# 10 · Verification ------------------------------------------------------------
###############################################################################

message("✅ Recent AMZN/ChrisCamillo check from twitter_raw_v2:")
try({
  print(DBI::dbGetQuery(con, "
    SELECT tweet_id, created_at, username, left(text, 160) AS text_preview
    FROM public.twitter_raw_v2
    WHERE username = 'ChrisCamillo'
      AND created_at >= now() - interval '3 days'
      AND text ILIKE '%AMZN%'
    ORDER BY created_at DESC
    LIMIT 10;
  "))
}, silent = TRUE)

message("✅ Done. Upserted twitter_raw_v2 + twitter_collapse_full at ", Sys.time())
