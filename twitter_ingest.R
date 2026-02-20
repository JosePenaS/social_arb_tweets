#!/usr/bin/env Rscript

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

# ── 0) ENV VARS ──────────────────────────────────────────────────────────────
BEARER <- Sys.getenv("X_BEARER")
if (BEARER == "") stop("Missing env var: X_BEARER")

SUPABASE_HOST <- Sys.getenv("SUPABASE_HOST")
SUPABASE_PORT <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
SUPABASE_DB   <- Sys.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER <- Sys.getenv("SUPABASE_USER")
SUPABASE_PWD  <- Sys.getenv("SUPABASE_PWD")

if (SUPABASE_HOST == "") stop("Missing env var: SUPABASE_HOST")
if (SUPABASE_USER == "") stop("Missing env var: SUPABASE_USER")
if (SUPABASE_PWD  == "") stop("Missing env var: SUPABASE_PWD")

# Optional: override the pooler host you were using
SUPABASE_POOLER_HOST <- Sys.getenv("SUPABASE_POOLER_HOST", "aws-0-us-east-2.pooler.supabase.com")

# Handles list (comma-separated) OR fallback list
handles <- trimws(strsplit(Sys.getenv(
  "TW_HANDLES",
  "ChrisCamillo,NolanAntonucci,ConsensusGurus,jordan_mclain,tickerplus,davehanson,AstutexAi,SingularityRes"
), ",")[[1]])
handles <- handles[nzchar(handles)]
if (length(handles) == 0) stop("No handles provided")

message("✅ Handles: ", paste(handles, collapse = ", "))

# ── Time window (manual override or anchored auto window) ────────────────────
end_time   <- Sys.getenv("X_END_TIME")
start_time <- Sys.getenv("X_START_TIME")

compute_window <- function(tz = "America/New_York") {
  now_local <- with_tz(Sys.time(), tz)

  today_0900 <- as.POSIXct(paste0(as.Date(now_local), " 09:00:00"), tz = tz)
  today_1630 <- as.POSIXct(paste0(as.Date(now_local), " 16:30:00"), tz = tz)
  yday_1630  <- today_1630 - days(1)

  # If we're before 09:00 → scrape from yesterday 16:30 → now
  # If we're between 09:00 and 16:30 → scrape from today 09:00 → now
  # If we're after 16:30 → scrape from today 16:30 → now
  start_local <- if (now_local < today_0900) {
    yday_1630
  } else if (now_local < today_1630) {
    today_0900
  } else {
    today_1630
  }

  list(
    start_time  = format(with_tz(start_local, "UTC"), "%Y-%m-%dT%H:%M:%SZ"),
    end_time    = format(with_tz(now_local,  "UTC"), "%Y-%m-%dT%H:%M:%SZ"),
    start_local = start_local,
    end_local   = now_local
  )
}

if (!nzchar(end_time) || !nzchar(start_time)) {
  tz_used <- Sys.getenv("LOCAL_TZ", "America/New_York")
  w <- compute_window(tz = tz_used)
  start_time <- w$start_time
  end_time   <- w$end_time
  message("✅ Window local: ", w$start_local, " → ", w$end_local, " (", tz_used, ")")
} else {
  message("✅ Window override (env): ", start_time, " → ", end_time)
}

message("✅ Window UTC:   ", start_time, " → ", end_time)

# ── 1) LOOKUP USER IDS ───────────────────────────────────────────────────────
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
  if (is.null(json$data)) return(tibble(username = character(), user_id = character()))

  tibble(
    username = vapply(json$data, \(x) x$username, ""),
    user_id  = vapply(json$data, \(x) x$id, "")
  )
}

accounts <- x_lookup_user_ids(handles, BEARER)
if (nrow(accounts) == 0) stop("No accounts returned from lookup")

# ── 2) FETCH TWEETS IN WINDOW (PAGINATED) ────────────────────────────────────
x_get_user_tweets_window <- function(user_id, bearer, start_time, end_time,
                                     max_pages = 50, sleep_s = 1,
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

    if (!is.null(token)) req <- req |> req_url_query(pagination_token = token)

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
      created_at = as.POSIXct(character()),
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

empty_posts <- tibble(
  id = character(),
  created_at = as.POSIXct(character()),
  text = character(),
  conversation_id = character(),
  referenced_tweets = list(),
  is_reply = logical(),
  is_quote = logical(),
  is_retweet = logical(),
  is_rt_text = logical()
)

posts_all <- pmap_dfr(
  list(accounts$user_id, accounts$username),
  function(uid, uname) {

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
        empty_posts
      }
    )

    if (nrow(res) == 0) return(empty_posts)

    res2 <- res %>%
      rowwise() %>%
      mutate(tmp = list(infer_flags(referenced_tweets)),
             is_reply   = tmp$is_reply,
             is_quote   = tmp$is_quote,
             is_retweet = tmp$is_retweet) %>%
      ungroup() %>%
      mutate(
        is_rt_text = str_detect(text %||% "", "^RT @"),
        username = uname,
        user_id = uid
      ) %>%
      select(-tmp)

    if (nrow(res2) == 0) empty_posts else res2
  }
)

posts_all <- posts_all %>% arrange(desc(created_at))

# ── 3) tweets_db ─────────────────────────────────────────────────────────────
tweets_db <- posts_all %>%
  transmute(
    tweet_id         = as.character(id),
    created_at       = ymd_hms(created_at, tz = "UTC"),
    username         = username,
    user_id          = user_id,
    text             = text,
    conversation_id  = as.character(conversation_id),
    is_reply         = as.logical(is_reply),
    is_quote         = as.logical(is_quote),
    is_retweet       = as.logical(is_retweet),
    is_rt_text       = as.logical(is_rt_text)
  ) %>%
  distinct(tweet_id, .keep_all = TRUE)

if (nrow(tweets_db) == 0) stop("No tweets fetched — aborting.")

# ── 4) fix conversation_id ───────────────────────────────────────────────────
fix_conversation_id <- function(df) {
  n <- nrow(df); if (!n) return(df)
  cid <- df$conversation_id
  iq  <- df$is_quote
  idx <- which(iq & (seq_len(n) < n))
  for (i in idx) {
    if (i + 2 <= n && !is.na(cid[i+1]) && cid[i + 1] == cid[i + 2]) cid[i] <- cid[i + 1]
    if (i + 1 <= n) cid[i + 1] <- cid[i]
  }
  df$conversation_id <- cid
  df
}

tidy_fix <- tweets_db %>%
  group_by(username) %>%
  group_modify(~ fix_conversation_id(.x)) %>%
  ungroup()

# ── 5) Collapse conversations ────────────────────────────────────────────────
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

# ── 6) Supabase upserts ──────────────────────────────────────────────────────
con <- dbConnect(
  RPostgres::Postgres(),
  host     = SUPABASE_POOLER_HOST,
  port     = SUPABASE_PORT,
  dbname   = SUPABASE_DB,
  user     = SUPABASE_USER,
  password = SUPABASE_PWD,
  sslmode  = "require"
)
on.exit(dbDisconnect(con), add = TRUE)

print(dbGetQuery(con, "select '✅ connected' as status, now();"))

# twitter_raw_v2
dbExecute(con, "
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

dbWriteTable(con, "tmp_twitter_raw_v2", tweets_db, temporary = TRUE, overwrite = TRUE)

dbExecute(con, "
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
dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_raw_v2;")

# twitter_collapse_full
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS public.twitter_collapse_full (
    collapse_id     text PRIMARY KEY,
    conversation_id text,
    username        text,
    created_at      timestamptz,
    text            text,
    n_tweets        integer
  );
")

dbWriteTable(con, "tmp_twitter_collapse_full", collapse_db, temporary = TRUE, overwrite = TRUE)

dbExecute(con, "
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
dbExecute(con, "DROP TABLE IF EXISTS tmp_twitter_collapse_full;")

message("✅ Done. Upserted twitter_raw_v2 + twitter_collapse_full at ", Sys.time())
