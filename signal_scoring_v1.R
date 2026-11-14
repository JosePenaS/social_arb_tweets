###############################################################################
# signal_scoring_v1.R
#
# Shared scoring definition for tweet trade signals (version "rescore_v1").
#
# This is the exact setup used by the historical re-score
# (historical_tweet_signal_rescore.Rmd -> twitter_trade_signals_rescore_v1):
#   - SYSTEM_PROMPT and make_tweet_prompt() copied verbatim
#   - ticker normalization (.fix_one / fix_ticker) copied verbatim
#
# Production (generate_tweet_signals.R) sources this file so live signals are
# scored exactly like the historical backtest. If you change anything here,
# bump SCORING_VERSION / PROMPT_VERSION so old and new scores are never mixed.
###############################################################################

SCORING_VERSION <- "rescore_v1"
PROMPT_VERSION  <- "tweet_signal_prompt_v1"
PARSER_VERSION  <- "tweet_signal_parser_v1"

# Model and settings used for the historical re-score.
SIGNAL_MODEL_DEFAULT     <- "gpt-4o-2024-11-20"
GENERATION_TEMPERATURE   <- 0
GENERATION_MAX_TOKENS    <- 1700L

# Tickers the re-score dropped after normalization.
DROP_TICKERS <- c("GOTR", "MIKE", "ISRO", "17B", "ALPHA", "AI", "TIN")

# ============================================================
# 7. SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT <-
  paste(
    "You are a concise analyst.",
    "Return exactly the JSON asked—no markdown."
  )


# ============================================================
# 8. ORIGINAL TWEET SIGNAL PROMPT
#
# IMPORTANT:
# DO NOT CHANGE THIS RUBRIC FOR THIS EXPERIMENT.
# ============================================================

make_tweet_prompt <- function(tweet_text) {

  safe_txt <- tweet_text |>

    stringr::str_replace_all(
      "\\{",
      "{{"
    ) |>

    stringr::str_replace_all(
      "\\}",
      "}}"
    )


  glue::glue("
You are a trading assistant that converts a single tweet into a *strict* JSON trade-idea list.

TWEET
-----
{safe_txt}
-----

TASK
----
1) Detect every stock / ETF / ADR ticker with enough substance for a trade idea.
   Use tidyquant/Yahoo symbols (e.g., AAPL, SHOP.TO, 7974.T, 9992.HK). Ignore $ prefixes.

2) Return EXACTLY one JSON object with a single key \"signals\" mapping to an array of idea objects.

3) Every idea inside \"signals\" MUST include ALL fields below and obey constraints:

| field              | type / range / rules                                                  |
|--------------------|-----------------------------------------------------------------------|
| ticker             | string (tidyquant symbol, no $)                                       |
| conviction         | float 0–1 (clip to [0,1])                                             |
| direction          | \"long\" | \"short\"                                                  |
| horizon            | \"short\" | \"mid\" | \"long\"                                        |
| stop_loss_pct      | integer 8–25 (clip to [8,25])                                         |
| buy_zone           | true | false                                                          |
| sentiment_score    | float −1 … 1 (clip to [−1,1])                                         |
| rationale          | string ≤ 60 words                                                     |
| specificity_score  | float 0–1  (see rubric; clip to [0,1])                                |
| has_quant_data     | boolean (true if tweet cites numbers, e.g., revenue, units, %s)       |
| evidence_score     | float 0–1  (see rubric; clip to [0,1])                                |
| evidence_types     | string of comma-separated tags from:                                  |
|                    |   financials, sales, guidance, macro, technical, news, stat,         |
|                    |   supply_chain, survey, alt_data, on_chain, insider, legal, other    |
| evidence_snippet   | ≤ 30 words quoted/paraphrased from tweet showing the evidence;        |
|                    |   empty string if none                                                |

4) **Specificity rubric** (how concrete the claim is):
   - 0.0–0.2: vague opinion (e.g., \"Tesla is the future\") no timeframe/catalyst
   - 0.3–0.5: directional view without quantities or timing
   - 0.6–0.8: concrete catalyst/timeframe/driver
   - 0.9–1.0: quantified forecast (numbers/dates/targets) or detailed mechanism

5) **Evidence rubric** (how well-supported it is in the TWEET itself):
   - 0.0: no evidence
   - 0.1–0.4: anecdote/weak reference (e.g., \"heard\", \"seems\")
   - 0.5–0.7: some numbers or specific sources
   - 0.8–1.0: multiple verifiable figures/sources or high-quality data

6) If an idea is too weak (e.g., no ticker OR specificity_score < 0.30) DROP it.
   If no valid ideas remain, return {{\"signals\": []}}.

7) Enforce all ranges by scaling/rounding/clipping. DO NOT include extra keys.
   DO NOT wrap the JSON in markdown.

")

}

# ============================================================
# 14. EXISTING TICKER NORMALIZATION
# ============================================================

.fix_one <- function(sym) {


  if (
    is.na(sym) ||
    stringr::str_trim(
      sym
    ) == ""
  ) {

    return(
      NA_character_
    )

  }


  sym <-
    sym %>%
    stringr::str_trim() %>%
    stringr::str_to_upper()


  stock_map <- c(

    CROCS = "CROX",
    TDMX = "TMDX",
    ATZ = "ATZ.TO",
    PUMA = "PUM.DE",
    HUGO = "BOSS.DE",
    PORTL = "PRPL",
    LNMD = "LNMD3.SA",
    DGHI = "DGHI.V",
    NEWS = "NWSA",
    ASICS = "7936.T",
    KRKN = "KRAKEN.V",
    GPS = "GPS",
    SHARKNINJA = "SN",
    CELSIUS = "CELH",
    RENK = "R5RK.DE",
    ARITZIA = "ATZ.TO",

    `KRX:278470` =
      "278470.KS",

    CAPCOM = "9697.T",
    LVMH = "MC.PA",
    DIAGEO = "DEO",
    LILY = "LLY",
    BUILD = "BLDR",
    AIRLINES = "JETS",
    TSMC = "TSM",
    GENR = "GNRC",
    POP = "9992.HK",
    ZYN = "PM",

    `2501.TSE` =
      "2501.T",

    `8050` =
      "8050.T",

    `973.XHKG` =
      "0973.HK",

    ADS = "ADS.DE",

    `ADS.GY` =
      "ADS.DE",

    ADYRY = "ASBRF",
    BRBY = "BRBY.L",
    NTODY = "NTDOY",

    `278470` =
      "278470.KS",

    `018290` =
      "018260.KS",

    LGHNH =
      "003550.KS",

    `LGHNH.KS` =
      "003550.KS",

    `EL.F` =
      "EL.PA",

    DOCK = "DOCK.L",

    `FOI-B` =
      "FOI-B.ST",

    GAW = "GAW.L",
    OIL = "USO",
    SP500 = "^GSPC",
    NASDAQ = "^IXIC",

    `BTC.X` =
      "BTC-USD",

    FB = "META",
    GGPI = "PSNY",
    GIK = "NVVE",
    LAC = "LAC",
    HEAR = "HEAR",
    MODN = "MODN",
    MPL = "MPL.AX",
    MTTR = "MTTR",
    NEWR = "NEWR",
    NIBE = "NIBE-B.ST",
    NVEI = "NVEI.TO",
    NOVO = "NVO",
    OZON = "OZON",
    OSTK = "OSTK",
    PDYPY = "PDYPY",
    PLNHF = "PLNHF",
    PYCR = "PYCR",
    QNT = "QNT.L",
    SAVE = "SAVE",
    SKL = "SKIL",
    SMAR = "SMAR",
    SNROY = "SNROF",
    SQ = "SQ",
    SQSP = "SQSP",
    TPX = "TPX",
    TRTN = "TRTN",
    TTCF = "TTCF",
    TWOU = "TWOU",
    TWTR = "TWTR",
    VSTO = "VSTO",
    VVNT = "VVNT",
    WIRE = "WIRE",
    WOSG = "WOSG.L",
    WWE = "TKO",
    YY = "YY",
    ZEV = "ZEV",

    `Z1P.AX` =
      "ZIP.AX",

    HYDG = "HGRAF",
    REDDIT = "RDDT",
    ADYEN = "ADYEN.AS",
    RPI = "RPI.L",
    PUIG = "PUIG.MC",
    ASMDEE_B = "ASMDEE-B.ST",
    SFER = "SFER.MI",
    PPGN = "PPGN.SW",

    `NUR.V` =
      "NUR.CV",

    CNDS = "CDNS",
    BANB = "BANB.SW",
    PARA = "PSKY",
    VKTK = "VKTX",

    BLG = "BLG.AX",
    IQE = "IQE.L",
    SIVE = "SIVE.ST",
    SOI = "SOI.PA",
    VNP = "VNP.TO",
    VCX = "VCX.AX",
    COW = "LE=F",

    `SK HYNIX` =
      "000660.KS",

    NITTOBO = "3110.T",
    ADVANTEST = "6857.T",
    AIXA = "AIXA.DE",
    AJINOMOTO = "2802.T",
    ASHM = "ASHM.L",

    `MITSUBISHI GAS CHEMICAL` =
      "4182.T",

    MXIC = "2337.TW",
    NANYA = "2408.TW",

    `SEIKOH GIKEN` =
      "6834.T",

    YAMAMURA = "5210.T",
    VGP = "VGP.BR",

    `2337.T` =
      "2337.TW",

    `TA-125` =
      "^TA125.TA",

    `TA-35` =
      "TA35.TA",

    SMIC = "0981.HK",
    PLSR = "PLSR.V",
    HY9H = "HY9H.F",
    VVST = "VST",

    BLONDESNMONEY =
      NA_character_,

    YELLOWBRICK =
      NA_character_,

    OPENAI =
      NA_character_,

    TRYCREATE =
      NA_character_,

    WSJ =
      NA_character_,

    CLICKHOUSE =
      NA_character_,

    CXMT =
      NA_character_,

    YMTC =
      NA_character_,

    WHOPZ =
      NA_character_,

    Z9 =
      NA_character_

  )


  if (
    sym %in%
      names(
        stock_map
      )
  ) {

    return(
      unname(
        stock_map[[sym]]
      )
    )

  }


  crypto_keep <- c(

    "BTC",
    "ETH",
    "ADA",
    "DOGE",
    "DOT",
    "AVAX",
    "LINK",
    "MANA",
    "1INCH",
    "AAVE",
    "APE",
    "ARB",
    "ALGO",
    "MASK",
    "METIS",
    "MILK",
    "MUTE",
    "MYRO",
    "NEXO",
    "NFT",
    "NFTX",
    "OKB",
    "OCEAN",
    "OXT",
    "PEPE",
    "PYR",
    "PYUSD",
    "RNDR",
    "RSR",
    "RVN",
    "SLERF",
    "SNEK",
    "LQTY",
    "FXS",
    "FRAX",
    "GAS",
    "GMX",
    "KAS",
    "ICP",
    "IMX",
    "ILV",
    "JASMY",
    "KDA",
    "KEEP",
    "DOCK",
    "MELD",
    "MOBILE",
    "DYM",
    "DOLA",
    "SAI",
    "LRC",
    "LUSD",
    "RENBTC",
    "REQ",
    "REP",
    "RLC",
    "RON",
    "AUDIO",
    "BAT",
    "CRO",
    "CVC",
    "DCR",
    "DERO",
    "DIONE",
    "GODS",
    "HNT",
    "ICX",
    "IOTX",
    "LPT",
    "LOOKS",
    "LOOM",
    "LADYS",
    "MKR",
    "MBL",
    "NKN",
    "NOIA",
    "ONT",
    "ORAI",
    "PAAL",
    "PAXG",
    "POLY",
    "PSYOP",
    "QUACK",
    "RAAS",
    "RETH",
    "REVV",
    "SFP",
    "SHIB",
    "SKL",
    "SPELL",
    "SSV",
    "STARL",
    "STEEM",
    "STRD",
    "STRK",
    "SUSHI",
    "SWETH",
    "SYS",
    "TAO",
    "TAROT",
    "TFUEL",
    "THETA",
    "TIA",
    "TLM",
    "TLOS",
    "TORN",
    "TOSHI",
    "UMA",
    "USDT",
    "VEE",
    "VELO",
    "VRSC",
    "WAXP",
    "WBNB",
    "WOO",
    "XAUT",
    "XCH",
    "XCN",
    "XDC",
    "XDEFI",
    "XEC",
    "XEM",
    "XLM",
    "XMR",
    "XPR",
    "XRD",
    "XRP",
    "XVG",
    "XYO",
    "ZANO",
    "ZEC",
    "ZEN"

  )


  if (
    sym %in%
      crypto_keep
  ) {

    return(
      paste0(
        sym,
        "-USD"
      )
    )

  }


  skip_syms <- c(

    "SOCIALARB",
    "SOLAMA",
    "TOKEN",
    "TICKERPLUS",
    "TIPRANKS",
    "TRUMP",
    "TTRENDS",
    "TRENDS",
    "NETVR",
    "PWEASE",
    "PUBLIC",
    "HOUSEHACK",
    "FIGMA",
    "FIGM",
    "FIGR",
    "FIGUREAI",
    "FIGURE.AI",
    "APPTRONIK",
    "SANCTUARY.AI",
    "OPENSEA",
    "MRBEAST",
    "KLAR",
    "KLARNA",
    "BOOPBAKERYCO",
    "BOBO",
    "BAYC",
    "SHIPPING",
    "VARDASPACE",
    "TEMU",
    "TES",
    "TULAV",
    "WLD",
    "WHOPZBET",
    "ZEPHYR_Z9"

  )


  if (
    sym %in%
      skip_syms
  ) {

    return(
      NA_character_
    )

  }


  sym

}


fix_ticker <- function(sym) {

  purrr::map_chr(
    sym,
    .fix_one
  )

}

# Prompt fingerprint, computed the same way as in the re-score notebook,
# so rows can be checked against the historical run.
compute_prompt_hash <- function() {
  if (!requireNamespace("digest", quietly = TRUE)) return(NA_character_)
  digest::digest(
    paste0(
      "SYSTEM_PROMPT:\n", SYSTEM_PROMPT,
      "\n\nMAKE_TWEET_PROMPT:\n",
      paste(deparse(body(make_tweet_prompt)), collapse = "\n")
    ),
    algo = "sha256",
    serialize = FALSE
  )
}
