# Symbol Directory — FTP Files and Definitions

Source: [NASDAQ Trader — Symbol Directory Definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)

The symbol directory files are published on the FTP and updated periodically throughout each day.

**FTP base directory:** `ftp://ftp.nasdaqtrader.com/symboldirectory/`

---

## Nasdaq-Listed Securities

| File | Description |
|------|--------------|
| **nasdaqlisted.txt** | Nasdaq-listed equity symbols and metadata |

**Market Category** (listing tier):

- **S** = Nasdaq Capital Market  
- **G** = Nasdaq Global Market℠  
- **Q** = Nasdaq Global Select Market℠  

**Financial Status** (listing/compliance):

- **N** = Normal (default)  
- **K** = Deficient, Delinquent, and Bankrupt  
- **J** = Delinquent and Bankrupt  
- **H** = Deficient and Delinquent  
- **G** = Deficient and Bankrupt  
- **Q** = Bankrupt  
- **E** = Delinquent  
- **D** = Deficient  

| Field | Definition |
|-------|------------|
| Symbol | 1–5 character identifier for each Nasdaq-listed security |
| Security Name | Company issuing the security |
| Market Category | S / G / Q (see above) |
| Test Issue | **Y** = test security, **N** = not test |
| Financial Status | N / K / J / H / G / Q / E / D (see above) |
| Round Lot | Number of shares in a round lot |
| *File Creation Time* | Last row: `File Creation Time: mmddyyyyhhmm\|\|\|\|\|` (timeliness of data) |

---

### Nasdaq When-Issue / When-Distributed

| File | Description |
|------|-------------|
| **NasdaqWhenIssueWhenDistributed.txt** | When-Issued and When-Distributed flags |

- **When Issued:** N = No, Y = Yes  
- **When Distributed:** N = No, Y = Yes  

| Field | Definition |
|-------|------------|
| Effective Date | Start-of-day date (filename date); when file was last updated |
| Issue Name | Issuer name |
| Symbol | Symbol |
| When Issued Flag | N / Y |
| When Distributed Flag | N / Y |

---

## Other Exchange-Listed Securities

| File | Description |
|------|-------------|
| **otherlisted.txt** | Securities listed on other exchanges (NYSE, NYSE ARCA, NYSE MKT, BATS, IEX, etc.) |

**Exchange** (listing market):

- **N** = New York Stock Exchange (NYSE)  
- **A** = NYSE MKT  
- **P** = NYSE ARCA  
- **Z** = BATS Global Markets (BATS)  
- **V** = Investors' Exchange, LLC (IEXG)  

**ACT Symbol** (effective 2/12/2010): identifier used in ACT and CTCI; typically 1–5 character root + 1–3 character suffix (up to 14 chars).  
**CQS Symbol**: identifier for SIAC CQS/CTS feeds (same convention).  
**Nasdaq Symbol** (effective 2/12/2010): identifier for Nasdaq protocols and market data (same convention).

**ETF:** **Y** = ETF, **N** = not ETF.  
**Test Issue:** **Y** = test security, **N** = not test.

| Field | Definition |
|-------|------------|
| ACT Symbol | ACT/CTCI identifier (see above) |
| Security Name | Name of security (type/class if applicable); up to 255 chars |
| Exchange | N / A / P / Z / V (see above) |
| CQS Symbol | CQS/CTS symbol |
| ETF | Y / N |
| Round Lot Size | Shares per round lot; up to 6 digits |
| Test Issue | Y / N |
| *File Creation Time* | Last row: timestamp as in other files |

---

## Market Participants

| File | Description |
|------|-------------|
| **mpidlist.txt** | Market participant IDs (MPIDs) and membership flags |

**Nasdaq Member:** Y / N  
**FINRA Member:** Y / N  
**Nasdaq Texas (NTX) Member:** Y / N  
**PSX Participant:** Y / N (PHLX member eligible to trade on PSX)

**MP Type:** A = Agency Quote, C = ECN, E = Exchange, M = Market Maker, N = Miscellaneous, O = Order Entry Firm, P = Nasdaq Participant, Q = Query Only Firm, S = Specialist

| Field | Definition |
|-------|------------|
| MPID | Market participant identifier (4 chars + optional 5th for branch) |
| MP Type | A / C / E / M / N / O / P / Q / S |
| Name | Firm name |
| Location | Location or trading desk name |
| Phone Number | Contact number |
| Nasdaq Member | Y / N |
| FINRA Member | Y / N |
| Nasdaq Texas Member | Y / N |
| PSX Participant | Y / N |
| *File Creation Time* | Last row: timestamp |

---

## Mutual Funds

| File | Description |
|------|-------------|
| **mfundslist.txt** | Mutual funds, money market funds, UITs, structured products, annuities |

**Type:** AN = Annuities, MF = Mutual Fund, MS = Supplemental Mutual Fund, $$ = Money Market Fund, $S = Supplemental Money Market Fund, SP = Structured Products, US = UIT Supplemental List, UT = UIT News Media List.

**Category** (subcategory) varies by type (e.g. for MF: O = Open-end, C = Closed-end; for MS: A / G / X; etc.).

| Field | Definition |
|-------|------------|
| Fund Symbol | Identifier for the fund |
| Fund Name | Name of the fund |
| Fund Family Name | Fund company name |
| Type | AN / MF / MS / $$ / $S / SP / US / UT |
| Category | Subcategory code |
| Pricing Agent | Firm responsible for pricing |
| *File Creation Time* | Last row: timestamp |

---

## PBOT Futures

| File | Description |
|------|-------------|
| **pbot.csv** | PBOT futures products |

**Path:** `ftp://ftp.nasdaqtrader.com/SymbolDirectory/pbot.csv`

| Field | Definition |
|-------|------------|
| Commodity ID | 3-character product identifier |
| PBOT Product Symbol | Commodity ID + 2-char expiration code |
| PBOT Product Description | Product name |
| Last Trade Date | Last trading day (yyyymmdd) |
| Expiration Date | Expiration date (yyyymmdd) |

---

## Nasdaq Options Market

| File | Description |
|------|-------------|
| **options.txt** | Options traded on the Nasdaq Options Market |

**Options Closing Type:** L = Late Hours, N = Normal Hours.  
**Options Type:** C = Call, P = Put.  
**Pending:** N = currently trading, Y = pending (next trading day).

| Field | Definition |
|-------|------------|
| Root Symbol | 1–6 character root for options with same underlying |
| Options Closing Type | L / N |
| Options Type | C / P |
| Expiration Date | mm/dd/yyyy |
| Explicit Strike Price | Strike at which contract may be exercised |
| Underlying Symbol | Underlying symbol in Nasdaq Options system |
| Underlying Issue Name | Issuer/issue description |
| Pending | N / Y |
| *File Creation Time* | Last row: timestamp |

---

## Nasdaq PHLX Equity Options

| File | Description |
|------|-------------|
| **phlxoptions.csv** | PHLX equity options |

**Path:** `ftp://ftp.nasdaqtrader.com/SymbolDirectory/phlxoptions.csv`

| Field | Definition |
|-------|------------|
| Company | Issuer name |
| Cycle | Expiration cycle (e.g. JAN, FEB, MAR; OTH for non-equity) |
| Option Symbol | 1–6 character root for options with same underlying |
| Stock Symbol | Underlying security symbol (1–5 chars) |
| Specialist Unit | Specialist firm name at Nasdaq PHLX |

---

## Nasdaq PHLX Options Strike Prices

| File | Description |
|------|-------------|
| **phlxstrikes.zip** | PHLX option strike data (zip) |

**Path:** `ftp://ftp.nasdaqtrader.com/symboldirectory/phlxstrikes.zip`

**CFI Code:** OP = Put, OC = Call. **Put Or Call:** P = Put, C = Call.

| Field | Definition |
|-------|------------|
| Symbol | Option root symbol |
| Equity Underlying | Primary underlying stock symbol |
| Phlx Underlying | PHLX underlying symbol |
| Maturity Year / Month / Day | Expiration date components |
| CFI Code | OP / OC |
| Strike Price Dollar / Decimal | Strike price |
| Put Or Call | P / C |

---

## Nasdaq | NTX | PSX Adds and Deletes

| File | Description |
|------|-------------|
| **TradingSystemAddsDeletes.txt** | Daily adds/deletes for Nasdaq, NTX, and PSX |

**Path:** `ftp://ftp.nasdaqtrader.com/dynamic/SymDir/TradingSystemAddsDeletes.txt`

**Nasdaq / NTX / PSX Action:** Add or Delete.  
**Primary Listing Market:** S = Nasdaq Capital Market, G = Nasdaq Global Market℠, Q = Nasdaq Global Select Market℠.

| Field | Definition |
|-------|------------|
| Symbol | 1–5 character listed security identifier |
| Company Name | Issuer name |
| Nasdaq Action | Add / Delete |
| NTX Action | Add / Delete |
| PSX Action | Add / Delete |
| Effective Date | Start-of-day date; when file was last updated |
| Primary Listing Market | S / G / Q |

---

## Files used in this module

This project typically uses:

- **nasdaqlisted.txt** — Nasdaq-listed symbols  
- **otherlisted.txt** — Other-exchange-listed symbols (NYSE, ARCA, BATS, IEX, etc.)  
- **mpidlist.txt** — Market participants (optional)

FTP directory: [ftp://ftp.nasdaqtrader.com/symboldirectory](ftp://ftp.nasdaqtrader.com/symboldirectory/)

---

## References

- [Symbol Look-Up](https://www.nasdaqtrader.com/Trader.aspx?id=symbollookup)
- [CQS Symbol Convention](https://www.nasdaqtrader.com/trader.aspx?id=CQSsymbolconvention)
- [Symbology FAQs — Dot/Suffix](https://www.nasdaqtrader.com/content/technicalsupport/specifications/symbology_faq.pdf)
- [Fifth Character Symbol Suffixes](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/nasdaqfifthcharactersuffixlist.pdf)
