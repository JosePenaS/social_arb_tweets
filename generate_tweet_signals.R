#!/usr/bin/env Rscript

###############################################################################
# generate_tweet_signals.R
# Generate structured trade signals from collapsed tweet conversations
#
# Scoring setup: identical to the historical re-score (see signal_scoring_v1.R)
#   - same model (gpt-4o-2024-11-20 by default), same prompt, same ticker rules
#   - temperature 0, so the same conversation always gets the same score
#
# Each conversation is scored ONCE. Every conversation sent to the model is
# recorded in twitter_signal_conversations_processed, including those that
# produce no signal, so they are never re-sent on later runs.
###############################################################################

cat("RUNNING FILE: generate_tweet_signals.R | GPT SIGNAL GENERATION\n")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(purrr)
  library(tibble)
  library(stringr)
  library(lubridate)
  library(jsonlite)
  library(httr2)
  library(glue)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

# Shared prompt, model settings and ticker normalization.
scoring_file <- if (file.exists("signal_scoring_v1.R")) "signal_scoring_v1.R" else "R/signal_scoring_v1.R"
source(scoring_file)

###############################################################################
# 0) Environment variables -----------------------------------------------------
###############################################################################

OPENAI_API_KEY <- Sys.getenv("OPENAI_API_KEY")
if (!nzchar(OPENAI_API_KEY)) stop("Missing env var: OPENAI_API_KEY")

SUPABASE_PORT <- as.integer(Sys.getenv("SUPABASE_PORT", "5432"))
SUPABASE_DB   <- Sys.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER <- Sys.getenv("SUPABASE_USER")
SUPABASE_PWD  <- Sys.getenv("SUPABASE_PWD")
SUPABASE_POOLER_HOST <- Sys.getenv("SUPABASE_POOLER_HOST")

if (!nzchar(SUPABASE_POOLER_HOST)) stop("Missing env var: SUPABASE_POOLER_HOST")
if (!nzchar(SUPABASE_USER)) stop("Missing env var: SUPABASE_USER")
if (!nzchar(SUPABASE_PWD))  stop("Missing env var: SUPABASE_PWD")

# Same model as the historical re-score unless explicitly overridden.
OPENAI_MODEL <- Sys.getenv("OPENAI_MODEL", SIGNAL_MODEL_DEFAULT)

MAX_CONVERSATIONS <- as.integer(Sys.getenv("MAX_CONVERSATIONS", "150"))
if (is.na(MAX_CONVERSATIONS) || MAX_CONVERSATIONS <= 0) MAX_CONVERSATIONS <- 150

# Only consider conversations from the last N days. Older unprocessed
# conversations are left alone (backfills are done with the re-score notebook).
SIGNAL_LOOKBACK_DAYS <- as.integer(Sys.getenv("SIGNAL_LOOKBACK_DAYS", "14"))
if (is.na(SIGNAL_LOOKBACK_DAYS) || SIGNAL_LOOKBACK_DAYS <= 0) SIGNAL_LOOKBACK_DAYS <- 14

SIGNALS_TABLE   <- Sys.getenv("SIGNALS_TABLE", "twitter_trade_signals")
SOURCE_TABLE    <- Sys.getenv("SOURCE_TABLE", "twitter_collapse_full")
PROCESSED_TABLE <- Sys.getenv("PROCESSED_TABLE", "twitter_signal_conversations_processed")

PROMPT_HASH <- compute_prompt_hash()

cat("OpenAI model:", OPENAI_MODEL, "\n")
cat("Temperature:", GENERATION_TEMPERATURE, "\n")
cat("Scoring version:", SCORING_VERSION, "| prompt hash:", PROMPT_HASH, "\n")
cat("Source table:", SOURCE_TABLE, "\n")
cat("Signals table:", SIGNALS_TABLE, "\n")
cat("Processed-conversations table:", PROCESSED_TABLE, "\n")
cat("Max conversations:", MAX_CONVERSATIONS, "| lookback days:", SIGNAL_LOOKBACK_DAYS, "\n")

###############################################################################
# 1) Supabase connection -------------------------------------------------------
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

on.exit({
  try(DBI::dbDisconnect(con), silent = TRUE)
}, add = TRUE)

print(DBI::dbGetQuery(con, "select 'connected' as status, now();"))

qid <- function(x) as.character(DBI::dbQuoteIdentifier(con, x))

target_tbl <- DBI::Id(schema = "public", table = SIGNALS_TABLE)
target_q <- as.character(DBI::dbQuoteIdentifier(con, target_tbl))

processed_q <- as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = "public", table = PROCESSED_TABLE)))

###############################################################################
# 2) Ensure target table exists ------------------------------------------------
###############################################################################

DBI::dbExecute(
  con,
  glue::glue_sql(
    "
    CREATE TABLE IF NOT EXISTS public.{`SIGNALS_TABLE`} (
      signal_id text PRIMARY KEY,
      username text,
      conversation_id text,
      created_at timestamptz,
      text text,
      ticker text,
      conviction double precision,
      direction text,
      horizon text,
      stop_loss_pct integer,
      buy_zone boolean,
      sentiment_score double precision,
      rationale text,
      specificity_score double precision,
      has_quant_data boolean,
      evidence_score double precision,
      evidence_types text,
      evidence_snippet text,
      loaded_at timestamptz DEFAULT now()
    );
    ",
    .con = con
  )
)

###############################################################################
# 2.1) Upgrade existing table schema if needed ---------------------------------
###############################################################################

signal_columns <- c(
  signal_id = "text",
  username = "text",
  conversation_id = "text",
  created_at = "timestamptz",
  text = "text",
  ticker = "text",
  conviction = "double precision",
  direction = "text",
  horizon = "text",
  stop_loss_pct = "integer",
  buy_zone = "boolean",
  sentiment_score = "double precision",
  rationale = "text",
  specificity_score = "double precision",
  has_quant_data = "boolean",
  evidence_score = "double precision",
  evidence_types = "text",
  evidence_snippet = "text",
  loaded_at = "timestamptz DEFAULT now()",
  # Scoring metadata: which setup produced each row. Rows written before
  # this change have NULL here (old prompt / gpt-4o-mini).
  ticker_raw = "text",
  collapse_id = "text",
  collapsed_n_tweets = "integer",
  scoring_version = "text",
  prompt_version = "text",
  prompt_hash = "text",
  parser_version = "text",
  model_requested = "text",
  generation_temperature = "double precision",
  gpt_raw = "text"
)

for (col in names(signal_columns)) {
  DBI::dbExecute(
    con,
    sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;", target_q, qid(col), signal_columns[[col]])
  )
}

###############################################################################
# 2.2) Create indexes safely ---------------------------------------------------
###############################################################################

safe_idx_base <- gsub("[^A-Za-z0-9_]", "_", SIGNALS_TABLE)

create_index <- function(suffix, column, unique = FALSE) {
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE %sINDEX IF NOT EXISTS %s ON %s(%s);",
      if (unique) "UNIQUE " else "",
      qid(paste0("idx_", safe_idx_base, "_", suffix)),
      target_q,
      column
    )
  )
}

create_index("signal_id_unique", "signal_id", unique = TRUE)
create_index("created_at", "created_at")
create_index("ticker", "ticker")
create_index("username", "username")
create_index("conversation", "conversation_id, username")

###############################################################################
# 2.3) Processed-conversations log ---------------------------------------------
#
# One row per conversation sent to the model, whether or not it produced a
# signal. Only successful model responses are recorded, so API failures are
# retried on the next run.
###############################################################################

DBI::dbExecute(
  con,
  sprintf(
    "
    CREATE TABLE IF NOT EXISTS %s (
      username         text NOT NULL,
      conversation_id  text NOT NULL,
      scoring_version  text NOT NULL,
      collapse_id      text,
      created_at       timestamptz,
      status           text,
      n_raw_signals    integer,
      n_valid_signals  integer,
      model_requested  text,
      prompt_hash      text,
      processed_at     timestamptz DEFAULT now(),
      PRIMARY KEY (username, conversation_id, scoring_version)
    );
    ",
    processed_q
  )
)

###############################################################################
# 3) Read collapsed conversations not already processed ------------------------
###############################################################################

source_exists <- DBI::dbExistsTable(con, DBI::Id(schema = "public", table = SOURCE_TABLE))
if (!source_exists) stop("Source table does not exist: public.", SOURCE_TABLE)

# A conversation is skipped if it already has a signal (from any scoring
# version) or was already processed by this scoring version.
sql <- glue::glue_sql(
  "
  SELECT
    c.collapse_id,
    c.conversation_id,
    c.username,
    c.created_at,
    c.text,
    c.n_tweets
  FROM public.{`SOURCE_TABLE`} c
  WHERE c.created_at >= now() - make_interval(days => {SIGNAL_LOOKBACK_DAYS})
    AND NOT EXISTS (
      SELECT 1
      FROM public.{`SIGNALS_TABLE`} s
      WHERE s.conversation_id = c.conversation_id
        AND s.username = c.username
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.{`PROCESSED_TABLE`} p
      WHERE p.conversation_id = c.conversation_id
        AND p.username = c.username
        AND p.scoring_version = {SCORING_VERSION}
    )
  ORDER BY c.created_at ASC
  LIMIT {MAX_CONVERSATIONS};
  ",
  .con = con
)

collapsed <- DBI::dbGetQuery(con, sql) %>%
  tibble::as_tibble() %>%
  mutate(
    created_at = as.POSIXct(created_at, tz = "UTC"),
    text = as.character(text),
    username = as.character(username),
    conversation_id = as.character(conversation_id),
    collapse_id = as.character(collapse_id),
    n_tweets = suppressWarnings(as.integer(n_tweets))
  )

cat("Collapsed conversations to process:", nrow(collapsed), "\n")

if (nrow(collapsed) == MAX_CONVERSATIONS) {
  cat(
    "NOTE: hit the MAX_CONVERSATIONS cap (", MAX_CONVERSATIONS,
    "). Remaining conversations will be processed on the next run.\n",
    sep = ""
  )
}

if (nrow(collapsed) == 0) {
  cat("No new collapsed conversations to process. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 4) OpenAI signal extraction --------------------------------------------------
###############################################################################

# Returns the raw model message (a JSON string), or NULL if the request failed
# after all retries. Request settings match the historical re-score.
call_openai_signals <- function(text, max_retries = 3) {
  body <- list(
    model = OPENAI_MODEL,
    temperature = GENERATION_TEMPERATURE,
    max_tokens = GENERATION_MAX_TOKENS,
    response_format = list(type = "json_object"),
    messages = list(
      list(role = "system", content = SYSTEM_PROMPT),
      list(role = "user", content = as.character(make_tweet_prompt(text)))
    )
  )

  last_error <- NULL

  for (attempt in seq_len(max_retries)) {
    resp <- tryCatch(
      request("https://api.openai.com/v1/chat/completions") |>
        req_headers(
          Authorization = paste("Bearer", OPENAI_API_KEY),
          `Content-Type` = "application/json"
        ) |>
        req_body_json(body, auto_unbox = TRUE) |>
        req_timeout(120) |>
        req_error(is_error = function(resp) FALSE) |>
        req_perform(),
      error = function(e) {
        last_error <<- e
        NULL
      }
    )

    if (!is.null(resp)) {
      status <- resp_status(resp)
      raw <- resp_body_string(resp)

      if (status >= 200 && status < 300) {
        parsed <- tryCatch(jsonlite::fromJSON(raw, simplifyVector = FALSE), error = function(e) NULL)
        content <- parsed$choices[[1]]$message$content %||% NULL

        if (!is.null(content) && nzchar(content)) {
          return(stringr::str_trim(content))
        }
      } else {
        message("OpenAI status ", status, ": ", substr(raw, 1, 500))
      }
    }

    wait_seconds <- min(60, 5 * attempt)
    message("OpenAI attempt ", attempt, " failed. Waiting ", wait_seconds, " seconds...")
    Sys.sleep(wait_seconds)
  }

  if (!is.null(last_error)) {
    message("Last OpenAI error: ", conditionMessage(last_error))
  }

  NULL
}

collapse_json_value <- function(x) {
  if (is.null(x)) return(NA_character_)
  out <- paste(as.character(unlist(x)), collapse = ", ")
  if (!nzchar(out)) NA_character_ else out
}

clip_num <- function(x, min_val, max_val) {
  out <- suppressWarnings(as.numeric(x %||% NA))
  if (length(out) != 1) out <- NA_real_
  pmin(pmax(out, min_val), max_val)
}

as_bool <- function(x) {
  if (is.null(x) || length(x) != 1) return(NA)
  if (is.logical(x)) return(x)
  y <- tolower(trimws(as.character(x)))
  if (y %in% c("true", "t", "1", "yes", "y")) TRUE else if (y %in% c("false", "f", "0", "no", "n")) FALSE else NA
}

# Converts the model's JSON into signal rows, following the re-score parser:
# values are clipped to their ranges, tickers go through fix_ticker(), and the
# FIGR rescue rule and drop list are applied. Rows also need a valid
# long/short direction, because the backtest requires one.
signals_to_tbl <- function(content, row) {
  out <- tryCatch(jsonlite::fromJSON(content, simplifyVector = FALSE), error = function(e) NULL)

  if (is.null(out)) {
    return(list(ok = FALSE, n_raw = 0L, signals = tibble()))
  }

  sigs <- out$signals %||% list()

  if (length(sigs) == 0) {
    return(list(ok = TRUE, n_raw = 0L, signals = tibble()))
  }

  rows <- map_dfr(seq_along(sigs), function(i) {
    s <- sigs[[i]]
    if (!is.list(s)) return(tibble())

    ticker_raw <- collapse_json_value(s$ticker)
    if (is.na(ticker_raw)) return(tibble())

    ticker <- fix_ticker(ticker_raw)

    # FIGR rescue rule from the re-score.
    if (is.na(ticker) && stringr::str_detect(row$text, "\\bFIGR\\b")) ticker <- "FIGR"

    if (is.na(ticker) || !nzchar(ticker) || ticker %in% DROP_TICKERS) return(tibble())

    direction <- tolower(trimws(collapse_json_value(s$direction)))
    if (is.na(direction) || !direction %in% c("long", "short")) return(tibble())

    signal_id <- paste(
      row$username,
      row$conversation_id,
      ticker,
      format(row$created_at, "%Y%m%d%H%M%S"),
      i,
      sep = ":"
    )

    tibble(
      signal_id = signal_id,
      username = row$username,
      conversation_id = row$conversation_id,
      created_at = row$created_at,
      text = row$text,
      ticker_raw = ticker_raw,
      ticker = ticker,
      conviction = clip_num(s$conviction, 0, 1),
      direction = direction,
      horizon = collapse_json_value(s$horizon),
      stop_loss_pct = suppressWarnings(as.integer(s$stop_loss_pct %||% NA)),
      buy_zone = as_bool(s$buy_zone),
      sentiment_score = clip_num(s$sentiment_score, -1, 1),
      rationale = collapse_json_value(s$rationale),
      specificity_score = clip_num(s$specificity_score, 0, 1),
      has_quant_data = as_bool(s$has_quant_data),
      evidence_score = clip_num(s$evidence_score, 0, 1),
      evidence_types = collapse_json_value(s$evidence_types),
      evidence_snippet = collapse_json_value(s$evidence_snippet),
      loaded_at = Sys.time(),
      collapse_id = row$collapse_id,
      collapsed_n_tweets = row$n_tweets,
      scoring_version = SCORING_VERSION,
      prompt_version = PROMPT_VERSION,
      prompt_hash = PROMPT_HASH,
      parser_version = PARSER_VERSION,
      model_requested = OPENAI_MODEL,
      generation_temperature = GENERATION_TEMPERATURE,
      gpt_raw = content
    )
  })

  list(ok = TRUE, n_raw = length(sigs), signals = rows)
}

###############################################################################
# 5) Process conversations -----------------------------------------------------
###############################################################################

results <- purrr::map(seq_len(nrow(collapsed)), function(i) {
  row <- collapsed[i, ]

  message("[", i, "/", nrow(collapsed), "] Processing ", row$username, " | ", row$conversation_id)

  content <- call_openai_signals(row$text)

  if (is.null(content)) {
    message("  Request failed; will retry on a later run.")
    return(list(signals = tibble(), processed = tibble()))
  }

  parsed <- signals_to_tbl(content, row)

  if (!parsed$ok) {
    message("  Could not parse model output: ", substr(content, 1, 300))
  } else {
    message("  Raw signals: ", parsed$n_raw, " | valid after cleaning: ", nrow(parsed$signals))
  }

  status <- if (!parsed$ok) {
    "parse_error"
  } else if (nrow(parsed$signals) > 0) {
    "signal"
  } else {
    "no_signal"
  }

  processed_row <- tibble(
    username = row$username,
    conversation_id = row$conversation_id,
    scoring_version = SCORING_VERSION,
    collapse_id = row$collapse_id,
    created_at = row$created_at,
    status = status,
    n_raw_signals = as.integer(parsed$n_raw),
    n_valid_signals = as.integer(nrow(parsed$signals)),
    model_requested = OPENAI_MODEL,
    prompt_hash = PROMPT_HASH,
    processed_at = Sys.time()
  )

  list(signals = parsed$signals, processed = processed_row)
})

all_signals <- bind_rows(purrr::map(results, "signals"))
processed_log <- bind_rows(purrr::map(results, "processed"))

cat("Conversations scored successfully:", nrow(processed_log), "of", nrow(collapsed), "\n")
cat("Total extracted signals:", nrow(all_signals), "\n")

###############################################################################
# 6) Upload/upsert signals -----------------------------------------------------
###############################################################################

upsert_table <- function(df, target, key_cols) {
  tmp_name <- paste0("tmp_", gsub("[^A-Za-z0-9_]", "_", target), "_", format(Sys.time(), "%Y%m%d%H%M%S"))

  DBI::dbWriteTable(con, tmp_name, df, temporary = TRUE, overwrite = TRUE, row.names = FALSE)

  tmp_q <- as.character(DBI::dbQuoteIdentifier(con, DBI::Id(table = tmp_name)))
  target_quoted <- as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = "public", table = target)))

  cols <- names(df)
  cols_q <- vapply(cols, qid, character(1))
  update_cols <- setdiff(cols, key_cols)

  update_set <- paste(
    vapply(update_cols, function(x) sprintf("%s = EXCLUDED.%s", qid(x), qid(x)), character(1)),
    collapse = ", "
  )

  DBI::dbExecute(
    con,
    sprintf(
      "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s;",
      target_quoted,
      paste(cols_q, collapse = ", "),
      paste(cols_q, collapse = ", "),
      tmp_q,
      paste(vapply(key_cols, qid, character(1)), collapse = ", "),
      update_set
    )
  )

  DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", tmp_q))
}

if (nrow(all_signals) > 0) {
  all_signals <- all_signals %>%
    distinct(signal_id, .keep_all = TRUE)

  upsert_table(all_signals, SIGNALS_TABLE, "signal_id")
  cat("Uploaded/upserted signals:", nrow(all_signals), "\n")

  print(all_signals %>% count(username, sort = TRUE))
} else {
  cat("No signals extracted this run.\n")
}

# Record processed conversations AFTER the signals are saved, so a failure
# while saving signals never marks a conversation as done.
if (nrow(processed_log) > 0) {
  upsert_table(processed_log, PROCESSED_TABLE, c("username", "conversation_id", "scoring_version"))
  cat("Recorded processed conversations:", nrow(processed_log), "\n")
  print(processed_log %>% count(status, sort = TRUE))
}

cat("generate_tweet_signals.R completed successfully.\n")
