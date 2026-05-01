# Honi Soit Corpus

*Written by Kelvin Lee — Sydney Corpus Lab (February 5, 2024)*

## Overview

This repository contains the **Honi Soit corpus**, a training dataset compiled using a variation of constructed week sampling. The corpus is approximately 60,000 words and comprises 100 news articles published by the University of Sydney student newspaper [Honi Soit](https://honisoit.com/category/news/) between January 2021 and December 2022.

| | 2021 | 2022 | Total |
|---|---|---|---|
| No. of articles | 50 | 50 | 100 |
| Number of tokens | 29,402 | 30,592 | 59,994 |

*Table 1: Honi Soit corpus description*

Articles come only from the News category section of the Honi Soit website and contain news reportage rather than non-news genres such as opinion or analysis.

## Purpose

This corpus was deliberately constructed as a small training dataset for use and potential distribution with text analytics notebooks developed as part of a collaboration on the:

- **Australian Text Analytics Platform (ATAP)**
- **Language Data Commons of Australia (LDaCA)**

Both are collaborative projects led by the University of Queensland and supported by the Australian Research Data Commons to develop infrastructure for researchers who work with language data.

## Sampling Method

News articles were selected using a variation of **constructed week sampling** — a type of stratified random sampling in which the complete sample represents all days of the week to account for cyclical variation of news content (Luke et al. 2011: 78).

One article was selected from each week within the January 2021–December 2022 timeframe. (No articles were sampled from the first week of January and final week of December, as Honi Soit does not publish during this period.)

Article selection began with the second last week of December 2022 and continued in reverse. An article is selected from a particular day of the week for one week, and then for the preceding week, an article is selected from a different day. For example, if a Wednesday article is selected for one week, the previous week's selection must fall on any day other than Wednesday.

The resulting corpus contains a roughly equal number of articles for each day of the week:

| | Mon | Tue | Wed | Thu | Fri | Sat | Sun |
|---|---|---|---|---|---|---|---|
| 2021 | 8 | 6 | 7 | 7 | 6 | 7 | 9 |
| 2022 | 6 | 8 | 8 | 7 | 8 | 7 | 6 |
| **Total** | **14** | **14** | **15** | **14** | **14** | **14** | **15** |

*Table 2: Days included in the constructed week sampling*

The number of constructed weeks included (~14) exceeds the minimum of two recommended by Hester and Dougall (2007: 820) for the content analysis of online news.

## Data Format

- Verbal text only — photos, visuals, and captions were removed during cleaning.
- Author, date, and update metadata were removed from article body text, but were used as the basis for file names.
- Each article is available as an individual `.txt` file with **UTF-8 encoding**.
- The complete dataset is also available as a **zipped file** for easy use with text analytics notebooks.

## Permissions

Prior to compiling the dataset, permission was sought from and granted by the Honi Soit editorial team to compile and distribute this dataset.

## Acknowledgments

We are grateful to the Honi Soit editorial team for giving us permission to compile and distribute this dataset.

## References

- Hester, J. B., and Dougall, E. (2007). 'The efficiency of constructed week sampling for content analysis of online news'. *Journalism & Mass Communication Quarterly* 84(4): 811–824.
- Luke, D. A., Caburnay, A., and Cohen, E. L. (2011). 'How much is enough? New recommendations for using constructed week sampling in newspaper content analysis of health stories'. *Communication Methods and Measures* 5(1): 76–91.
