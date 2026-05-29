#!/usr/bin/env Rscript

###############################################################################
# generate_tweet_signals.R
# Generate structured trade signals from collapsed tweet conversations
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

OPENAI_MODEL <- Sys.getenv("OPENAI_MODEL", "gpt-4o-mini")

MAX_CONVERSATIONS <- as.integer(Sys.getenv("MAX_CONVERSATIONS", "150"))
if (is.na(MAX_CONVERSATIONS) || MAX_CONVERSATIONS <= 0) MAX_CONVERSATIONS <- 150

SIGNALS_TABLE <- Sys.getenv("SIGNALS_TABLE", "twitter_trade_signals")
SOURCE_TABLE  <- Sys.getenv("SOURCE_TABLE", "twitter_collapse_full")

cat("OpenAI model:", OPENAI_MODEL, "\n")
cat("Source table:", SOURCE_TABLE, "\n")
cat("Signals table:", SIGNALS_TABLE, "\n")
cat("Max conversations:", MAX_CONVERSATIONS, "\n")

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

DBI::dbExecute(
  con,
  glue::glue_sql(
    "CREATE INDEX IF NOT EXISTS idx_{`SIGNALS_TABLE`}_created_at ON public.{`SIGNALS_TABLE`}(created_at);",
    .con = con
  )
)

DBI::dbExecute(
  con,
  glue::glue_sql(
    "CREATE INDEX IF NOT EXISTS idx_{`SIGNALS_TABLE`}_ticker ON public.{`SIGNALS_TABLE`}(ticker);",
    .con = con
  )
)

DBI::dbExecute(
  con,
  glue::glue_sql(
    "CREATE INDEX IF NOT EXISTS idx_{`SIGNALS_TABLE`}_username ON public.{`SIGNALS_TABLE`}(username);",
    .con = con
  )
)

###############################################################################
# 3) Read collapsed conversations not already processed ------------------------
###############################################################################

source_exists <- DBI::dbExistsTable(con, DBI::Id(schema = "public", table = SOURCE_TABLE))
if (!source_exists) stop("Source table does not exist: public.", SOURCE_TABLE)

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
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.{`SIGNALS_TABLE`} s
    WHERE s.conversation_id = c.conversation_id
      AND s.username = c.username
  )
  ORDER BY c.created_at DESC
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
    conversation_id = as.character(conversation_id)
  )

cat("Collapsed conversations to process:", nrow(collapsed), "\n")

if (nrow(collapsed) == 0) {
  cat("No new collapsed conversations to process. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

###############################################################################
# 4) OpenAI signal extraction --------------------------------------------------
###############################################################################

system_prompt <- paste(
  "You are an expert stock-market signal extraction assistant.",
  "You read social-media text and extract actionable trade ideas.",
  "Return strict JSON only.",
  "Only include a signal if the text clearly implies a bullish or bearish trade idea.",
  "Do not invent tickers.",
  "If no valid trade signal exists, return {\"signals\":[]}.",
  sep = "\n"
)

make_user_prompt <- function(username, created_at, text) {
  paste0(
    "Extract trade signals from this tweet/thread.\n\n",
    "Username: ", username, "\n",
    "Created at UTC: ", created_at, "\n\n",
    "Text:\n",
    text,
    "\n\n",
    "Return JSON with this exact structure:\n",
    "{\n",
    "  \"signals\": [\n",
    "    {\n",
    "      \"ticker\": \"AAPL\",\n",
    "      \"conviction\": 0.7,\n",
    "      \"direction\": \"long\",\n",
    "      \"horizon\": \"short\",\n",
    "      \"stop_loss_pct\": 15,\n",
    "      \"buy_zone\": true,\n",
    "      \"sentiment_score\": 0.6,\n",
    "      \"rationale\": \"Brief reason, max 60 words.\",\n",
    "      \"specificity_score\": 0.7,\n",
    "      \"has_quant_data\": true,\n",
    "      \"evidence_score\": 0.6,\n",
    "      \"evidence_types\": \"sales,alt_data,news\",\n",
    "      \"evidence_snippet\": \"short evidence quote or summary\"\n",
    "    }\n",
    "  ]\n",
    "}\n\n",
    "Field rules:\n",
    "- ticker: ticker only, no dollar sign. Use Yahoo-style suffix if present, e.g. BRBY.L.\n",
    "- conviction: number from 0 to 1.\n",
    "- direction: only long or short.\n",
    "- horizon: only short, mid, or long.\n",
    "- stop_loss_pct: integer from 8 to 25.\n",
    "- buy_zone: true if the text suggests current/near-term entry is attractive; otherwise false.\n",
    "- sentiment_score: number from -1 bearish to 1 bullish.\n",
    "- specificity_score: 0 to 1, higher if the signal is specific and concrete.\n",
    "- has_quant_data: true if there is numeric evidence, charts, sales, growth, valuation, etc.\n",
    "- evidence_score: 0 to 1, higher if evidence is strong.\n",
    "- evidence_types: comma-separated tags from sales,guidance,technical,stat,news,alt_data,macro,financials,legal,insider,survey,supply_chain,other.\n",
    "- evidence_snippet: max 30 words.\n"
  )
}

call_openai_signals <- function(username, created_at, text, max_retries = 3) {
  body <- list(
    model = OPENAI_MODEL,
    response_format = list(type = "json_object"),
    messages = list(
      list(role = "system", content = system_prompt),
      list(role = "user", content = make_user_prompt(username, created_at, text))
    ),
    temperature = 0.1
  )
  
  last_error <- NULL
  
  for (attempt in seq_len(max_retries)) {
    resp <- tryCatch(
      request("https://api.openai.com/v1/chat/completions") |>
        req_headers(
          Authorization = paste("Bearer", OPENAI_API_KEY),
          `Content-Type` = "application/json"
        ) |>
        req_body_json(body) |>
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
          out <- tryCatch(jsonlite::fromJSON(content, simplifyVector = FALSE), error = function(e) NULL)
          if (!is.null(out)) return(out)
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
  
  list(signals = list())
}

clean_direction <- function(x) {
  x <- tolower(trimws(as.character(x)))
  ifelse(x %in% c("long", "short"), x, NA_character_)
}

clean_horizon <- function(x) {
  x <- tolower(trimws(as.character(x)))
  ifelse(x %in% c("short", "mid", "long"), x, "mid")
}

clean_num <- function(x, default = NA_real_, min_val = -Inf, max_val = Inf) {
  out <- suppressWarnings(as.numeric(x))
  ifelse(is.na(out), default, pmin(pmax(out, min_val), max_val))
}

clean_bool <- function(x) {
  if (is.logical(x)) return(x)
  y <- tolower(trimws(as.character(x)))
  y %in% c("true", "t", "1", "yes", "y")
}

normalize_ticker <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x <- gsub("^\\$", "", x)
  x <- gsub("[^A-Z0-9.\\-]", "", x)
  x
}

signals_to_tbl <- function(openai_out, row) {
  sigs <- openai_out$signals %||% list()
  
  if (length(sigs) == 0) {
    return(tibble())
  }
  
  map_dfr(seq_along(sigs), function(i) {
    s <- sigs[[i]]
    
    ticker <- normalize_ticker(s$ticker %||% NA_character_)
    direction <- clean_direction(s$direction %||% NA_character_)
    
    if (is.na(ticker) || !nzchar(ticker) || is.na(direction)) {
      return(tibble())
    }
    
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
      ticker = ticker,
      conviction = clean_num(s$conviction %||% 0.5, default = 0.5, min_val = 0, max_val = 1),
      direction = direction,
      horizon = clean_horizon(s$horizon %||% "mid"),
      stop_loss_pct = as.integer(round(clean_num(s$stop_loss_pct %||% 15, default = 15, min_val = 8, max_val = 25))),
      buy_zone = clean_bool(s$buy_zone %||% FALSE),
      sentiment_score = clean_num(s$sentiment_score %||% 0, default = 0, min_val = -1, max_val = 1),
      rationale = as.character(s$rationale %||% ""),
      specificity_score = clean_num(s$specificity_score %||% 0.5, default = 0.5, min_val = 0, max_val = 1),
      has_quant_data = clean_bool(s$has_quant_data %||% FALSE),
      evidence_score = clean_num(s$evidence_score %||% 0.2, default = 0.2, min_val = 0, max_val = 1),
      evidence_types = as.character(s$evidence_types %||% "other"),
      evidence_snippet = as.character(s$evidence_snippet %||% ""),
      loaded_at = Sys.time()
    )
  })
}

###############################################################################
# 5) Process conversations -----------------------------------------------------
###############################################################################

all_signals <- purrr::map_dfr(seq_len(nrow(collapsed)), function(i) {
  row <- collapsed[i, ]
  
  message("[", i, "/", nrow(collapsed), "] Processing ", row$username, " | ", row$conversation_id)
  
  out <- call_openai_signals(
    username = row$username,
    created_at = row$created_at,
    text = row$text
  )
  
  sig_tbl <- signals_to_tbl(out, row)
  
  message("  Signals extracted: ", nrow(sig_tbl))
  
  sig_tbl
})

cat("Total extracted signals:", nrow(all_signals), "\n")

if (nrow(all_signals) == 0) {
  cat("No signals extracted. Exiting successfully.\n")
  quit(save = "no", status = 0)
}

all_signals <- all_signals %>%
  distinct(signal_id, .keep_all = TRUE)

###############################################################################
# 6) Upload/upsert signals -----------------------------------------------------
###############################################################################

tmp_name <- paste0("tmp_", SIGNALS_TABLE, "_", format(Sys.time(), "%Y%m%d%H%M%S"))

DBI::dbWriteTable(
  con,
  tmp_name,
  all_signals,
  temporary = TRUE,
  overwrite = TRUE,
  row.names = FALSE
)

tmp_q <- as.character(DBI::dbQuoteIdentifier(con, DBI::Id(table = tmp_name)))
target_q <- as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = "public", table = SIGNALS_TABLE)))

cols <- names(all_signals)
cols_q <- vapply(cols, qid, character(1))
update_cols <- setdiff(cols, "signal_id")

update_set <- paste(
  vapply(
    update_cols,
    function(x) sprintf("%s = EXCLUDED.%s", qid(x), qid(x)),
    character(1)
  ),
  collapse = ", "
)

upsert_sql <- sprintf(
  "
  INSERT INTO %s (%s)
  SELECT %s
  FROM %s
  ON CONFLICT (signal_id) DO UPDATE
  SET %s;
  ",
  target_q,
  paste(cols_q, collapse = ", "),
  paste(cols_q, collapse = ", "),
  tmp_q,
  update_set
)

DBI::dbExecute(con, upsert_sql)

cat("Uploaded/upserted signals:", nrow(all_signals), "\n")

print(
  all_signals %>%
    count(username, sort = TRUE)
)

cat("generate_tweet_signals.R completed successfully.\n")
