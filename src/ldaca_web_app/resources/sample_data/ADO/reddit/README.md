# 2025 Australian Federal Election NewsTalk/Reddit Dataset

This document outlines the methodology for collecting and processing data from Reddit and NewsTalk for the 2025 Australian Federal Election. 
This collection contains both raw (de-identified) social media data and AI-generated labels for topic area, political party/figure/policy mentions and support/opposition sentiment. The dataset and labels are intended to address a broad research question: "Which issues are most salient to the Australian public in the lead-up to the 2025 Federal Election, and how do these issues relate to political parties and support/opposition sentiment?".

The social media content in this dataset was classified as "potentially political", e.g. Reddit submissions and Australian news media stories that explicitly mention the election, political parties, or candidates, or are ostensibly about topics which are likely to be salient during the election, particularly given the collection timeframe between 25 March 2025 and 9 May 2025, corresponding to these events:

- Budget week preceding the election announcement
- Five weeks of the election campaign up to the morning of the election date
- The election date itself
- One week of data collected after the election date

This dataset also includes weekly election reports produced by a cascading summarisation process that focuses on major topic/policy areas, and also a free form 'other' category intended to capture topics outside of those identified. The election reports contain counts of news story/submission summaries that contributed to each topic area which is useful for understanding the relative salience of each topic area. However the reports are not citable research data, they are intended to serve as data exploration. The full data and coding labels are intended to be used for research purposes.

Simply put, if you intend to explore a particular topic area, you should look at the reports to get a sense of the prominence of the topic you're interested in. Then you can use the raw comment data, selecting the data coded for the topic of interest. It is likely useful to retrieve the id of the story/submission that you are interested in, and then use that to retrieve the comments associated with it. Alternatively, for Reddit at least, you can just view the comment thread on Reddit itself.

## The Digital Observatory

This dataset was prepared by the [Digital Observatory](https://www.digitalobservatory.net.au/) (the DO), a research infrastructure unit at Queensland University of Technology (QUT). The Digital Observatory is dedicated to collecting, processing, and analyzing human data on the web for research purposes. The DO has also been pioneering the use of generative AI to augment large-scale data collection and analysis. 

In conjunction with the Australian Internet Observatory, the DO has two research platforms salient to this dataset:
- [AusReddit](https://ausreddit.digitalobservatory.net.au/): A platform that collects Australian Reddit data.
- [NewsTalk](https://newstalk.digitalobservatory.net.au/): A platform that collects Australian news media commentary and linked Reddit discussions.

## Preparatory work

Prior to AI processing, we prepared a list of topic areas that are likely to be salient during the election campaign. This list was developed based on a combination of mining election manifestos, and a first-pass thematic analysis of Reddit data from major political subreddits. There are 56 topic areas grouped into 9 broad categories.

- Other
    - General Political Disillusionment and Cynicism
    - Media Bias and Influence Concerns
    - Role and Influence of Minor Parties and Independents
    - Critiques of Peter Dutton and Comparisons to Trump
    - Influence of Political Donations and Lobbying
    - Political Polling Analysis and Skepticism
    - Concerns about the Brisbane Olympics
    - Debate on Truth in Political Advertising Legislation
    - Work From Home Policies (Political Context)
    - Live Animal Export Ethical Debate
- Economic Policy
    - Cost of Living Concerns
    - Tax Policy Debates
    - Government Spending, Debt, and Fiscal Management
    - Energy Policy (Economic Aspects)
    - Supermarket Pricing and Regulation
    - Public Transport Affordability
- National Security
    - Community Safety, Policing, and Justice
    - Racism, Xenophobia, and Immigrant Integration
    - Immigration's Impact on Housing and Infrastructure
    - Defence Strategy and Foreign Relations
    - Border Security, Deportation, and Asylum Policies
- Housing & Infrastructure
    - Housing Affordability Crisis & Generational Impact
    - Government Housing Policy Critique
    - Housing Supply, Density & Development Challenges
    - Infrastructure Debates and Major Projects
    - Immigration's Role in Housing Pressure
- Climate & Energy
    - Renewable Energy Implementation Challenges
    - Environmental Concerns
    - Cost of Living & Energy Prices
    - Nuclear vs. Renewable Energy Debate
    - Gas Policy Debate
    - Electric Vehicles (EVs) & Fuel Policy
- Healthcare
    - Medicare, Bulk Billing & Affordability
    - Hospital System Strain & Access
    - Mental Healthcare Access & Funding
    - Dental Care Access & Medicare Inclusion
    - Vaccination & Public Health Issues
    - Smoking/Vaping Policy & Health Impacts
    - Women's Health Initiatives & Access
- Social Services
    - Welfare Payment Adequacy and Access
    - NDIS Concerns (Cost, Rorting, Reform)
    - Homelessness and Housing Support Challenges
    - Public Sector Workforce and Service Delivery Concerns
    - Worker Rights, Wages, and Union Role
    - Childcare Costs and Accessibility
- Industry & Manufacturing
    - Desire for Domestic Production ('Made in Australia')
    - Resource Management, Value-Adding, Foreign Ownership
    - Concern over Manufacturing Decline & Reliance on Imports
    - Government Initiatives for Industry Development
    - Economic and Social Impacts of Live Sheep Export Ban
    - Impact of Energy Costs on Competitiveness
- Education & Skills
    - Student Debt & Tertiary Affordability
    - Public School Funding & Resources
    - Challenges in the Teaching Profession & School Environment
    - Early Childhood Education Access & Quality
    - Role and Impact of International Students

For Reddit submissions we augmented the data by using a multimodal AI model to describe image posts. 

## Sources and volumes

Note: This section describes the volume of data in this dataset. The volume of available data on the sources we describe is much larger, but we have limited the data to politically relevant content. See the procedure section for details on how we selected the data.

- **Total Comments Across Both Datasets**: 188,682
- **Reddit Share**: 56.6% (106,840 comments)
- **Newstalk Share**: 43.4% (81,842 comments)
- **Reddit Average Words per Comment**: 65.2
- **Newstalk Average Words per Comment**: 40.6

### Reddit

We identified 20 politically relevant Australian subreddits. This list is not exhaustive but captures the most popular political subreddits and ensures state representation. The subreddits are:

- **AustralianPolitics**: 29,457 comments
- **australia**: 27,541 comments
- **australian**: 11,687 comments
- **perth**: 6,920 comments
- **brisbane**: 6,613 comments
- **melbourne**: 4,912 comments
- **AusPol**: 4,653 comments
- **queensland**: 3,645 comments
- **Adelaide**: 2,560 comments
- **circlejerkaustralia**: 1,625 comments
- **AusEcon**: 1,447 comments
- **AusPublicService**: 1,287 comments
- **canberra**: 1,154 comments
- **sydney**: 1,050 comments
- **WesternAustralia**: 750 comments
- **aus**: 662 comments
- **tasmania**: 347 comments
- **hobart**: 298 comments
- **darwin**: 215 comments
- **aboriginal**: 17 comments

Top 10 submissions by comment volume:

- **US will impose a minimum baseline tariff of 10 per cent on Australian imports to US**: 756 comments
- **A thought on the election result - did we just see the voter demographic tipping point?**: 444 comments
- **Labor to pledge $2.3 billion to subsidise home batteries**: 431 comments
- **Unrealised capital gains tax - why is no one talking more about this?**: 423 comments
- **Greens leader Adam Bandt concedes defeat in seat of Melbourne**: 418 comments
- **Peter Dutton says he wants house prices to 'steadily increase' to protect home owners**: 417 comments
- **The Greens had a shit Saturday. But Labor deluded if it thinks voters rejected the party**: 412 comments
- **Big preferential vote swing against Bandt as postal votes get counted**: 396 comments
- **Prime Minister Anthony Albanese suggests Greens responsible for own fall, attacks Max Chandler-Mathe...**: 391 comments
- **Megathread: Final Leaders Debate**: 378 comments

Data was collected directly through the Reddit API.

### NewsTalk

The NewsTalk data was harvested from eight publishers (larger number of distinct mastheads). 

- **Sky News (sky)**: 36,624 comments
- **Nine Entertainment (nine)**: 25,411 comments
- **The Guardian Australia (guardia)**: 11,255 comments
- **MSN News (msn)**: 6,369 comments
- **Independent Australia (ia)**: 1,091 comments
- **The Riot Act (riotact)**: 716 comments
- **Perth Now (perthnow)**: 224 comments
- **Australian Community Media (acm)**: 152 comments

The names in brackets are the values in the source column for this publisher.

Note, researchers may find that focusing on a grouping of Sky and MSN, vs Nine and Guardian, is useful for understanding the political leanings of the sources. Sky and MSN are generally considered to be more conservative, while Nine and Guardian are more left-leaning.

Data was collected via a direct connection to the NewsTalk database.

## Dataset Comparison

- **Total Comments Across Both Datasets**: 188,682
- **Reddit Share**: 56.6% (106,840 comments)
- **Newstalk Share**: 43.4% (81,842 comments)
- **Reddit Average Words per Comment**: 65.2
- **Newstalk Average Words per Comment**: 40.6

## Ethical considerations

All the raw data in this dataset is publicly available on Reddit and various Australian news websites. Nevertheless, we have anonymized the data by replacing author names with deterministic hashed values (e.g., "anon_abc123") to protect user privacy. Nevertheless, it is relatively straightforward to find the original posts on Reddit or NewsTalk (note that the submissions and stories data contain URLs to the original data where the comment threads can be accessed), so we caution against reproducing potentially controversial content in publications. 

## AI processing discussion

We utilised Google Gemini Flash 2.0 and an experimental Flash 2.5 for the majority of AI tasks in this work:

1. **Political Classification**: Classifying Reddit submissions and NewsTalk stories as potentially political or non-political.
2. **Topic Classification**: Classifying Reddit comments and NewsTalk comments into topic areas based on the list of 56 topic areas grouped into 9 broad categories.
3. **Party and Support Classification**: Identifying political party mentions and support/opposition sentiment in comments.
4. **Weekly Report Generation**: First stage of the cascading summarisation process to produce weekly reports based on the Reddit data.

We used Google Gemini Pro 2.0 for the second stage of the cascading summarisation process to produce detailed summaries for each topic area, and then synthesise a report from these summaries.

We are often asked why we use Gemini for this work versus other commercial models or open weight models. This is a rapidly evolving area, and our choice of model is based on a combination of factors including cost, performance, reliability. Performance and reliability are particularly important when processing large volumes of data, as we do in this work. 

In addition, context length and long-context performance are important factors. So too, maximum output tokens. In the latter case, our pipeline switched from Gemini Flash 2.0 to the newer experimental Flash 2.5 when the data being processed would exhaust the output token limit of Flash 2.0 (8k tokens). Flash 2.5 has a 64k output token limit. 

Another question that comes up is why we use a commercial model versus an open weight model. In an ideal world we would prefer the reproducibility of an open weight model, in that we can maintain a copy of the model's weights. Our choice in using a commercial model is primarily down to performance. The computational requirements to self-host a model of sufficient capability to process the volume of data we work with is feasible (QUT's HPC facility, for example) but the performance would be lower in terms of capability, particularly given the lack of long context support, and the data processing time. 

We are often asked about the coding reliability of the models we use, or 'accuracy' to put it another way. In other projects we go to great lengths to address coding instructions (prompt engineering) and human validation. That is not the case here. We did apply the usual approaches to prompt engineering over a subset of data with human validation but only to the extent we were 'reasonably' happy with the AI's results. The AI makes mistakes, many of them. For this type of job, so do humans. What humans can't do is apply a consistent coding scheme to hundreds of thousands of comments more or less in real-time during an election. We think the accuracy is better than 80%. That is the point at which we decided to stop iterating on the prompts. The most contentious labels in this work are the party support labels, which typically infer political party support based on the context of the comment. 

## Representing comments: Different approaches for Reddit and NewsTalk

Reddit comments are typically longer than NewsTalk comments. This often provides enough context in a single isolated comment to apply the coding labels when we also supply the submission title and selftext. NewsTalk comments are often much shorter, and often do not provide enough context to apply the coding labels. This is a familiar problem in social media data processing, and for the NewsTalk dataset we applied a new technique to represent comments in a threaded structure via markdown bullet lists (which LLMs have been trained on extensively). This takes a form like this:

```
A NewsTalk story title.
Comments:
1. Comment 1 text
2. Comment 2 text
  3. A reply to comment 2
    4. A reply to the reply
```

Coupled with threaded coding instructions like this:

```
The comments are provided in a nested structure, with each comment numbered (the comment id). The nested comments are indented to show the hierarchy. This can be useful for understanding the context beyond the text of the comment itself. For example, if a comment makes clearly identifies a topic, and a reply (indented) comment doesn't mention the topic but appears to engage in the topic then you must code the reply with the same topic(s) as the comment it is replying to. You should also differentiate which topics a reply is engaging with. In the case of ambiguity, then assign no topics. For example if a comment refers to multiple topics and a reply simply says "I agree", we cannot tell which topic the comment is agreeing with and no topic should be assigned to the reply. However if the reply says "I agree with your point about the cost of living" then you can assign the cost of living topic to the reply.
```

## Data format

Both the Reddit and NewsTalk data are provided in Parquet format for submissions/stories, and comments. If you are not familiar with Parquet, it is a columnar storage format that is efficient for analytical workloads. Typically one would load these files directly into Pandas dataframes via the `pd.read_parquet()` function in Python. The data is structured as follows:

### Reddit

The columns in `reddit_comments.parquet` are:

- **comment_id** (`str`): Unique identifier for the comment.
- **submission_id** (`str`): Unique identifier for the Reddit submission (post) the comment belongs to.
- **submission_title** (`str`): Title of the Reddit submission.
- **comment_body** (`str`): Text content of the comment.
- **created_utc** (`datetime64[ns]`): UTC timestamp of when the comment was created.
- **comment_author** (`str`): Anonymized string representing the comment author.
- **subreddit** (`str`): Name of the subreddit where the comment was posted.
- **score** (`int64`): Reddit score (upvotes minus downvotes) for the comment.
- **topic** (`list[str]`): *AI determined* list of topic label codes assigned to the comment.
- **party** (`str`): *AI determined Topic* political party label assigned to the comment (e.g., "ALP", "LNP", "GREENS", "OTHER", "ALL", or "NONE").
- **support** (`str`): *AI determined support* or opposition sentiment label for the identified party ("SUPPORT", "OPPOSE", or "NONE").
- **week** (`int64`): Integer representing the week number in the collection period.

The columns in `reddit_submissions.parquet` are:

• **submission_id** (`str`): Unique identifier for the Reddit submission
• **submission_author** (`str`): Anonymized username of the submission author
• **title** (`str`): Title of the Reddit submission
• **selftext** (`str`): Self-text content of the submission
• **url** (`str`): URL linked by the submission
• **created_utc** (`datetime64[ns]`): UTC timestamp when the submission was created
• **permalink** (`str`): Reddit permalink to the submission
• **score** (`int64`): Reddit score/karma for the submission
• **subreddit** (`str`): Name of the subreddit where the submission was posted
• **num_comments** (`int64`): Number of comments on the submission
• **harvested_at** (`datetime64[ns]`): UTC timestamp when the submission was harvested
• **augment_status** (`str`): Status of content augmentation (nullable)
• **image_desc** (`str`): AI-generated description of linked images (nullable)
• **url_title** (`str`): Title extracted from linked URLs (nullable)
• **political_reason** (`str`): Reason why the submission was classified as political
• **week** (`str`): Campaign week identifier

### Reddit (Election Reports)

A series of weekly reports are also included in the dataset. These reports are based exclusively on the Reddit data, and are intended to provide a high-level overview of the political topics discussed during the election campaign. 

The reports are provided as text files formatted in MarkDown.

### NewsTalk

The columns in `newstalk_comments.parquet` are:

- **comment_id** (`str`): Unique identifier for the comment.
- **story_id** (`str`): Unique identifier for the NewsTalk story id the comment belongs to.
- **story_title** (`str`): Title of the Reddit submission.
- **comment_body** (`str`): Text content of the comment.
- **created_utc** (`datetime64[ns]`): UTC timestamp of when the comment was created.
- **comment_author** (`str`): Anonymized string representing the comment author.
- **source** (`str`): Name of the source (publisher) where the comment was posted (e.g., "sky", "nine", "guardian", etc.).
- **parent_id** (`str`): Unique identifier for the parent comment if this is a reply, otherwise `None`.
- **topic** (`list[str]`): *AI determined* list of topic label codes assigned to the comment.
- **party** (`str`): *AI determined Topic* political party label assigned to the comment (e.g., "ALP", "LNP", "GREENS", "OTHER", "ALL", or "NONE").
- **support** (`str`): *AI determined support* or opposition sentiment label for the identified party ("SUPPORT", "OPPOSE", or "NONE").
- **week** (`int64`): Integer representing the week number in the collection period.

The columns in `newstalk_stories.parquet` are:

• **story_id** (`str`): Unique identifier for the NewsTalk story
• **story_author** (`str`): Author of the news story (anonymized if anonymization enabled)
• **title** (`str`): Title of the news story
• **description** (`str`): Extended description of the story
• **url** (`str`): URL of the original news story
• **image_url** (`str`): URL of associated image (nullable)
• **created_utc** (`datetime64[ns]`): UTC timestamp when the story was published
• **source** (`str`): News source/publication name
• **political_reason** (`str`): Reason why the story was classified as political
• **week** (`str`): Campaign week identifier

### Weeks

The `week` field does not correspond directly to calendar weeks, but rather to specific periods in the election campaign. Week 6 is the week after the election, and Week 1 is longer, covering the lead-up to the election announcement (including the release of the budget). The weeks are defined as follows:

| Week | Start Date & Time (AEST)      | End Date & Time (AEST)        | Notes                                      |
|------|-------------------------------|-------------------------------|--------------------------------------------|
| 1    | 24 March 2025, 08:00          | 4 April 2025, 07:59           | Pre-campaign period & Campaign Week 1      |
| 2    | 4 April 2025, 08:00           | 11 April 2025, 07:59          | Campaign week 2                            |
| 3    | 11 April 2025, 08:00          | 18 April 2025, 07:59          | Campaign week 3                            |
| 4    | 18 April 2025, 08:00          | 25 April 2025, 07:59          | Campaign week 4                            |
| 5    | 25 April 2025, 08:00          | 3 May 2025, 07:59             | Campaign week 5 up to election morning     |
| 6    | 3 May 2025, 08:00             | 10 May 2025, 08:00            | Post-election week                         |

All times are in Australian Eastern Standard Time (AEST).

### Figures

There are two figures we have included in the dataset:

1. Political Sentiment by Party: This is a list of the top 20 topic areas, with each topic area offering three bars for ALP, LNP and GREENS. The stacked values represent party sentiment extracted from comments coded for each topic area.
2. Political Engagement: This is a list of the top 20 topic areas with a simple bar which indicates the portion of comments coded for each topic area which mentioned a political party, figure or clearly political policy. This is intended to give a sense of the relative salience of each topic area in terms of political engagement for each week.

We provide figures for the Reddit and NewsTalk data separately in subdirectories:

- `figures/newstalk/`
- `figures/reddit/`

Example:
![Week 1 NewsTalk weekly sentiment by party](figures/newstalk/week1_sentiment_by_party.png)

## Procedure

The data collection and processing procedure involves several steps, which are outlined below

1. We harvest the raw submissions (Reddit) and news story metadata (NewsTalk) for the time period of interest
2. We utilise AI to classify submissions and stories that are 'potentially political'
3. We harvest Reddit comments and NewsTalk comments from the political submissions and stories
4. We classify the comments using AI to identify topic areas, political party/figure/policy mentions, and support/opposition sentiment

These are the steps that produce the annotated data in the parquet files in this collection.

We also used a cascading AI summarisation strategy to produce weekly reports during the election campaign. We include the weekly reports in this collection as well, but they are not intended to be used as research data. They are intended to be used for data exploration and understanding the relative salience of different topic areas during the election campaign.

The steps to produce the weekly reports are as follows:

1. We produce topic summaries for each submission (e.g. summaries of comments tagged by broad topic area)
2. We iterate through the topic list and produce a detailed summary from each topic area 
3. We use a generative AI model to synthesize a report from the summaries of each topic area, and the prior report output for weeks > 1

## Prompts

The following section includes the generative AI prompts used in data processing.

### Political Classification

These prompts are intended to classify Reddit submissions and NewsTalk stories as potentially political or non-political. The classification is based on the presence of overt political content, mention of political parties, politicians, policies, or topics likely to be discussed during the election campaign. The purpose of this phase/classification is to reduce the volume of data to a manageable size for further analysis.

#### Reddit
```
Your task is to analyze a number of Reddit submissions and classify them as potentially political (true) or non-political (false). You will be given a list of submissions with a submission title, and a text body and image description if these are present.

These submissions are from Australian subreddits during the 2025 Federal election campaign period. Our goal is to ultimately identify submissions that are likely to prompt political discussion. Obviously submissions that exclictly mention the election, political parties, politicians or policies should be classified as potentially political. However 'potentially political' includes submissions in key topic areas where we would expect submission discussion to be political during a federal election campaign. Accordingly, you are provided list of topics that are likely to be discussed in the context of the election. Additionally, submissions that are clearly satirical or humorous in nature but are related to these topics should also be classified as potentially political. If submissions are related to these topics, they should be classified as potentially political:

Economic Policy

- Tax Reform: Tax cuts, review of government spending
- Cost of Living Support: Energy bill relief, lowering inflation
- Economic Regulation: Penalizing anti-competitive behaviors, breaking up supermarket duopoly
- Student Debt Management: Reduction or elimination of student debt
- Public Transport: Affordable public transport initiatives

Healthcare

- Medicare Enhancement: Bulk billing improvements, increased GP numbers, dental inclusion
- Specialized Health Services: Urgent care clinics, mental health support
- Women's Health: Specialized clinics for conditions like endometriosis, cancer initiatives

Housing & Infrastructure

- Housing Affordability: Mass home building, affordable housing initiatives
- Foreign Investment Regulation: Temporary bans on foreign investment in existing homes
- Rental Market Protection: Rent increase limitations, tenant protection authorities
- Immigration Policy: Migration adjustments to address housing pressure
- Government Construction: Direct government intervention in housing construction

Climate & Energy

- Renewable Energy Targets: Grid transformation goals, investment in renewable infrastructure
- Energy Source Diversification: Nuclear energy, gas reservation, public-owned renewables
- Environmental Conservation: Forest protection, endangered species preservation
- Cost of Living Implications: Energy policy impacts on household expenses

Education & Skills

- Tertiary Education Access: Free TAFE, student debt policies
- Early Education: Improved access to early education, universal childcare proposals
- School Support Systems: Public school funding, financial support for families

Social Services

- Welfare Support: Centrelink payment adjustments, pension reforms, retirement age policies
- Child Care Accessibility: Childcare assistance programs, universal options
- Worker Protections: Employment rights and protections

National Security & Immigration

- Border Security: Criminal deportation policies, anti-smuggling measures
- Migration Management: Adjustments to migration intake, international student policies
- Community Safety: Bail law reforms, youth crime initiatives

Industry & Manufacturing

- Domestic Production: "Made in Australia" initiatives, local purchasing support
- Industry Transition: Job creation during climate and economic transitions

INSTRUCTIONS:
1. Classify each submission as political (true) or non-political (false).
2. Provide brief reasoning for your decision, e.g. overt political content or topic drawn from the list provided
3. Output JSON providing the submission id, political classification and the reason for classifying the submission as political or non-political.

SUBMISSIONS:
```

### NewsTalk

```
Your task is to analyze a number of News stories and classify them as potentially political (true) or non-political (false). You will be given a list of stories with a title and description.

These stories are from Australian online newspapers/news sites during the 2025 Federal election campaign period. Our goal is to stories that are likely to prompt political discussion. Stories that exclictly mention the election, political parties, politicians or policies should be classified as potentially political. However 'potentially political' includes stories in key topic areas where we would expect comments on these stories to be political during a federal election campaign. Accordingly, you are provided list of topics that are likely to be discussed in the context of the election. Additionally, stories that are clearly satirical or humorous in nature but are related to these topics should also be classified as political. If stories are related to these topics, they should be classified as political:

Economic Policy

- Tax Reform: Tax cuts, review of government spending
- Cost of Living Support: Energy bill relief, lowering inflation
- Economic Regulation: Penalizing anti-competitive behaviors, breaking up supermarket duopoly
- Student Debt Management: Reduction or elimination of student debt
- Public Transport: Affordable public transport initiatives

Healthcare

- Medicare Enhancement: Bulk billing improvements, increased GP numbers, dental inclusion
- Specialized Health Services: Urgent care clinics, mental health support
- Women's Health: Specialized clinics for conditions like endometriosis, cancer initiatives

Housing & Infrastructure

- Housing Affordability: Mass home building, affordable housing initiatives
- Foreign Investment Regulation: Temporary bans on foreign investment in existing homes
- Rental Market Protection: Rent increase limitations, tenant protection authorities
- Immigration Policy: Migration adjustments to address housing pressure
- Government Construction: Direct government intervention in housing construction

Climate & Energy

- Renewable Energy Targets: Grid transformation goals, investment in renewable infrastructure
- Energy Source Diversification: Nuclear energy, gas reservation, public-owned renewables
- Environmental Conservation: Forest protection, endangered species preservation
- Cost of Living Implications: Energy policy impacts on household expenses

Education & Skills

- Tertiary Education Access: Free TAFE, student debt policies
- Early Education: Improved access to early education, universal childcare proposals
- School Support Systems: Public school funding, financial support for families

Social Services

- Welfare Support: Centrelink payment adjustments, pension reforms, retirement age policies
- Child Care Accessibility: Childcare assistance programs, universal options
- Worker Protections: Employment rights and protections

National Security & Immigration

- Border Security: Criminal deportation policies, anti-smuggling measures
- Migration Management: Adjustments to migration intake, international student policies
- Community Safety: Bail law reforms, youth crime initiatives

Industry & Manufacturing

- Domestic Production: "Made in Australia" initiatives, local purchasing support
- Industry Transition: Job creation during climate and economic transitions

INSTRUCTIONS:
1. Classify each story as political (true) or non-political (false).
2. Provide brief reasoning for your decision, e.g. overt political content, topic drawn from the list provided etc.
3. Output JSON with a stories key set to a list of dictonaries, with story_id int, poltical bool, and reason str key/values.

STORIES:
```

### Topic Classification

These prompts are intended to classify Reddit comments and NewsTalk comments into topic areas. The classification is based on the presence of specific topics that are likely to be discussed during the election campaign. Note that this prompt includes the list of topics 56 topic areas grouped into 9 broad categories, as described in the prepatory work section, but here we provide coding instructions for each.

#### Reddit
```
# Instructions
Your task is to analyze the comments for a Reddit submission related to the 2025 Australian Federal Election and code (label) comments according the code book we provide. The code book is a list of topics that are likely to be discussed in the context of the election which include the topics you will look for.

Each code book entry contains the code and instructions which describe when to apply each code. The instructions contain specific examples but are not exhaustive. You should use your judgment to apply the codes to the comments based on the instructions provided. CAREFULLY CONSIDER all potential codes based on these instructions, and include all relevant codes for each comment. 

You will also provide a reason string for code assignment. The reason should be a brief selection of keywords, topic indicators, etc., used to identify this code. Try be terse and not include a whole comment unless it is necessary to illustrate multiple codes. Reason should be an empty empty string when no codes have been assigned to a comment.

## Code book
~~~json
[
    {
        "code": "OTHER-DISILLUSION",
        "instructions": "Code comments expressing a general lack of faith in major political parties, politicians, or the political system. This includes mentions of career politicians, perceived corruption, politicians being out of touch, and dissatisfaction with the two-party system.",
        "name": "Other - General Political Disillusionment and Cynicism"
    },
    {
        "code": "OTHER-MEDIA",
        "instructions": "Code comments discussing the perceived bias of media outlets (e.g., News Corp, ABC) and their influence on political narratives and public opinion. Include discussions about media objectivity and its role in shaping politics.",
        "name": "Other - Media Bias and Influence Concerns"
    },
    {
        "code": "OTHER-MINOR_INDP",
        "instructions": "Code comments discussing the role, potential impact, or appeal of minor parties (e.g., Greens, One Nation) and independent candidates (e.g., Teals). Include discussions on strategic voting and the possibility of a hung parliament.",
        "name": "Other - Role and Influence of Minor Parties and Independents"
    },
    {
        "code": "OTHER-DUTTON_CRITIQUE",
        "instructions": "Code comments specifically criticizing Opposition Leader Peter Dutton's policies, leadership style, or perceived alignment with Donald Trump's political tactics. Include mentions of specific controversies related to Dutton (e.g., Kirribilli House preference, views on working from home).",
        "name": "Other - Critiques of Peter Dutton and Comparisons to Trump"
    },
    {
        "code": "OTHER-DONATIONS_LOBBY",
        "instructions": "Code comments expressing concern about the influence of corporate donations and lobbyists on political decision-making and democratic representation.",
        "name": "Other - Influence of Political Donations and Lobbying"
    },
    {
        "code": "OTHER-POLLING",
        "instructions": "Code comments discussing, analyzing, or expressing skepticism about political polls, their methodology, accuracy, or predictive power.",
        "name": "Other - Political Polling Analysis and Skepticism"
    },
    {
        "code": "OTHER-OLYMPICS",
        "instructions": "Code comments debating the Brisbane Olympics, focusing on aspects like costs, impact on infrastructure and housing, public sentiment, and perceived wastefulness.",
        "name": "Other - Concerns about the Brisbane Olympics"
    },
    {
        "code": "OTHER-TRUTH_ADS",
        "instructions": "Code comments discussing the need for, or debate surrounding, legislation to ensure truthfulness in political advertising.",
        "name": "Other - Debate on Truth in Political Advertising Legislation"
    },
    {
        "code": "OTHER-WFH",
        "instructions": "Code comments discussing work-from-home policies, particularly for public servants, often linked to political figures or perceived hypocrisy.",
        "name": "Other - Work From Home Policies (Political Context)"
    },
    {
        "code": "OTHER-LIVE_EXPORT_ETHICS",
        "instructions": "Code comments focusing on the ethical and controversial aspects of the live animal export trade, including debates surrounding the proposed ban on live sheep exports.",
        "name": "Other - Live Animal Export Ethical Debate"
    },
    {
        "code": "ECON-COST_LIVING",
        "instructions": "Code comments expressing concern about the rising cost of living, focusing on the affordability of essentials like groceries, energy, housing, and transport. Include expressions of economic anxiety and hardship.",
        "name": "Economic Policy - Cost of Living Concerns"
    },
    {
        "code": "ECON-TAX",
        "instructions": "Code comments discussing or debating tax policy. This includes income tax cuts, fuel excise changes, bracket creep, fairness of the tax system, and potential taxes on corporations or wealth.",
        "name": "Economic Policy - Tax Policy Debates"
    },
    {
        "code": "ECON-SPENDING_DEBT",
        "instructions": "Code comments scrutinizing government spending, fiscal responsibility, national debt levels, the funding and economic impact of infrastructure projects, public service efficiency, and proposed cuts to the public service.",
        "name": "Economic Policy - Government Spending, Debt, and Fiscal Management"
    },
    {
        "code": "ECON-ENERGY_POLICY",
        "instructions": "Code comments discussing the economic aspects of energy policy. This includes the cost implications of renewables versus fossil fuels/nuclear, gas reservation policies, and the direct impact of energy policy on household bills and the economy.",
        "name": "Economic Policy - Energy Policy (Economic Aspects)"
    },
    {
        "code": "ECON-SUPERMARKET",
        "instructions": "Code comments discussing supermarket pricing, perceived price gouging, market power of the duopoly (Coles/Woolworths), and calls for government regulation or intervention.",
        "name": "Economic Policy - Supermarket Pricing and Regulation"
    },
    {
        "code": "ECON-TRANSPORT_AFFORD",
        "instructions": "Code comments discussing the affordability, cost, and investment in public transport, particularly as it relates to cost of living pressures.",
        "name": "Economic Policy - Public Transport Affordability"
    },
    {
        "code": "NATSEC-SAFETY_JUSTICE",
        "instructions": "Code comments discussing community safety issues, crime rates (especially youth crime), the effectiveness and conduct of policing, sentencing, bail laws, and perceived problems within the justice system.",
        "name": "National Security - Community Safety, Policing, and Justice"
    },
    {
        "code": "NATSEC-RACISM_INTEGRATION",
        "instructions": "Code comments discussing societal attitudes towards immigrants, challenges of integration, cultural clashes, racism, or xenophobia, particularly in the context of immigration debates.",
        "name": "National Security - Racism, Xenophobia, and Immigrant Integration"
    },
    {
        "code": "NATSEC-IMMIG_HOUS_INFRA",
        "instructions": "Code comments linking immigration levels (high or low) directly to pressures on housing availability, affordability, infrastructure capacity, and related cost of living impacts, framed as an immigration/population management issue.",
        "name": "National Security - Immigration's Impact on Housing and Infrastructure"
    },
    {
        "code": "NATSEC-DEFENCE_FOREIGN",
        "instructions": "Code comments discussing national security strategy, defence spending, military alliances (e.g., AUKUS), relationships with other countries (e.g., US, China), nuclear submarines, foreign espionage, foreign influence, and Australia's strategic positioning.",
        "name": "National Security - Defence Strategy and Foreign Relations"
    },
    {
        "code": "NATSEC-BORDER_ASYLUM",
        "instructions": "Code comments discussing border control policies, deportation of non-citizens (especially criminals), management of asylum seekers, boat arrivals, offshore detention, and migration data accuracy.",
        "name": "National Security - Border Security, Deportation, and Asylum Policies"
    },
    {
        "code": "HOUS-AFFORD_CRISIS",
        "instructions": "Code comments expressing concern about the high cost and difficulty of affording housing (both buying and renting). Include discussions on the impact on younger generations, intergenerational tension, and feelings of hopelessness regarding home ownership or rental security.",
        "name": "Housing & Infrastructure - Housing Affordability Crisis & Generational Impact"
    },
    {
        "code": "HOUS-GOV_POLICY",
        "instructions": "Code comments discussing, debating, or criticizing government housing policies. Include mentions of specific initiatives (e.g., HAFF), tax settings (negative gearing, CGT discounts), foreign investment rules, lending standards, and skepticism about policy effectiveness or political will.",
        "name": "Housing & Infrastructure - Government Housing Policy Critique"
    },
    {
        "code": "HOUS-SUPPLY_DEV",
        "instructions": "Code comments focusing on issues related to housing supply. Include discussions on the need for more housing, urban density debates, zoning laws, NIMBYism, construction industry challenges, and land release.",
        "name": "Housing & Infrastructure - Housing Supply, Density & Development Challenges"
    },
    {
        "code": "HOUS-INFRA_PROJECTS",
        "instructions": "Code comments discussing general infrastructure issues (e.g., transport reliability, road congestion) or specific large-scale infrastructure projects (e.g., Melbourne Airport Rail Link, T2D tunnel, Brisbane Olympics infrastructure), including their costs, benefits, funding, and planning.",
        "name": "Housing & Infrastructure - Infrastructure Debates and Major Projects"
    },
    {
        "code": "HOUS-IMMIG_IMPACT",
        "instructions": "Code comments discussing the role of immigration and population growth as a factor contributing to housing demand, housing shortages, affordability issues, and strain on infrastructure, framed from the perspective of housing outcomes.",
        "name": "Housing & Infrastructure - Immigration's Role in Housing Pressure"
    },
    {
        "code": "CLIM-RENEWABLE_IMPL",
        "instructions": "Code comments discussing the practical aspects and challenges of transitioning to renewable energy. Include mentions of grid integration and upgrades, energy storage solutions (e.g., batteries), ensuring reliability, and related infrastructure.",
        "name": "Climate & Energy - Renewable Energy Implementation Challenges"
    },
    {
        "code": "CLIM-ENVIRON_CONCERN",
        "instructions": "Code comments expressing broader environmental concerns. Include discussions on habitat destruction, land clearing, mining impacts, pollution, biodiversity loss, species protection, and the need for action to meet climate change targets.",
        "name": "Climate & Energy - Environmental Concerns"
    },
    {
        "code": "CLIM-ENERGY_COST",
        "instructions": "Code comments explicitly linking climate and energy policies (renewables, gas, nuclear, subsidies, carbon pricing) to the cost of electricity and gas for households and businesses.",
        "name": "Climate & Energy - Cost of Living & Energy Prices"
    },
    {
        "code": "CLIM-NUCLEAR_VS_RENEW",
        "instructions": "Code comments comparing and debating the merits, costs, feasibility, environmental impacts, and timelines of pursuing nuclear power versus accelerating renewable energy development.",
        "name": "Climate & Energy - Nuclear vs. Renewable Energy Debate"
    },
    {
        "code": "CLIM-GAS_POLICY",
        "instructions": "Code comments discussing gas policy. Include debates on domestic gas reservation, gas exports, the role of gas as a transition fuel, gas prices, and the environmental impact of gas extraction and use (e.g., fracking).",
        "name": "Climate & Energy - Gas Policy Debate"
    },
    {
        "code": "CLIM-EV_FUEL",
        "instructions": "Code comments discussing electric vehicles (EVs), including adoption rates, charging infrastructure, government incentives or taxes (e.g., road user charges), vehicle emission standards, and the future of fuel excise.",
        "name": "Climate & Energy - Electric Vehicles (EVs) & Fuel Policy"
    },
    {
        "code": "HEALTH-MEDICARE_AFFORD",
        "instructions": "Code comments discussing Medicare, particularly the availability and decline of bulk billing, out-of-pocket costs for GP and specialist visits, and the general affordability of accessing healthcare services through Medicare.",
        "name": "Healthcare - Medicare, Bulk Billing & Affordability"
    },
    {
        "code": "HEALTH-HOSPITALS",
        "instructions": "Code comments discussing issues within the hospital system. Include mentions of overcrowded emergency departments (EDs), long wait times for appointments or surgeries, ambulance ramping, urgent care clinics, and perceived inadequacy of hospital resources or funding.",
        "name": "Healthcare - Hospital System Strain & Access"
    },
    {
        "code": "HEALTH-MENTAL",
        "instructions": "Code comments discussing the accessibility and affordability of mental health services. Include mentions of difficulty finding practitioners, cost barriers, long wait times, insufficient Medicare rebates, and the need for better funding or integration.",
        "name": "Healthcare - Mental Healthcare Access & Funding"
    },
    {
        "code": "HEALTH-DENTAL",
        "instructions": "Code comments discussing the high cost and difficulty of accessing dental care. Include calls for dental services to be included under Medicare and mentions of people avoiding or delaying treatment due to cost.",
        "name": "Healthcare - Dental Care Access & Medicare Inclusion"
    },
    {
        "code": "HEALTH-VAX_PUBLIC",
        "instructions": "Code comments discussing public health matters such as vaccination rates, management of infectious diseases (e.g., measles, flu), vaccine hesitancy, public health campaigns, and related government policies.",
        "name": "Healthcare - Vaccination & Public Health Issues"
    },
    {
        "code": "HEALTH-SMOKE_VAPE",
        "instructions": "Code comments discussing government policies related to tobacco and vaping. Include mentions of taxes, regulations, availability, black markets, and public health consequences or debates.",
        "name": "Healthcare - Smoking/Vaping Policy & Health Impacts"
    },
    {
        "code": "HEALTH-WOMENS",
        "instructions": "Code comments discussing specific health issues and services relevant to women. Include mentions of funding for specific conditions (e.g., endometriosis), access to reproductive healthcare (e.g., abortion), maternity services, and perceived historical underfunding.",
        "name": "Healthcare - Women's Health Initiatives & Access"
    },
    {
        "code": "SOC-WELFARE_PAYMENTS",
        "instructions": "Code comments discussing the adequacy of social welfare payments like JobSeeker, pensions, or other Centrelink benefits. Include mentions of payment rates relative to poverty levels, eligibility criteria, partner income tests, and difficulties dealing with Centrelink/Services Australia.",
        "name": "Social Services - Welfare Payment Adequacy and Access"
    },
    {
        "code": "SOC-NDIS",
        "instructions": "Code comments discussing the National Disability Insurance Scheme (NDIS). Include mentions of its sustainability, management, rising costs, potential misuse or rorting, and calls for reform, while acknowledging its necessity.",
        "name": "Social Services - NDIS Concerns (Cost, Rorting, Reform)"
    },
    {
        "code": "SOC-HOMELESSNESS",
        "instructions": "Code comments discussing homelessness and the challenges faced by homeless individuals in accessing shelter, support services, and the adequacy of the social safety net.",
        "name": "Social Services - Homelessness and Housing Support Challenges"
    },
    {
        "code": "SOC-PUBLIC_SVC_DELIVERY",
        "instructions": "Code comments expressing concern about the public service workforce, potential job cuts, and the resulting impact on the quality, timeliness, or accessibility of government services (e.g., Centrelink, Medicare, DVA).",
        "name": "Social Services - Public Sector Workforce and Service Delivery Concerns"
    },
    {
        "code": "SOC-WORKER_RIGHTS",
        "instructions": "Code comments discussing employment conditions and protections. Include mentions of fair wages, wage theft, casualization, insecure work, the role and importance of unions, and Fair Work regulations.",
        "name": "Social Services - Worker Rights, Wages, and Union Role"
    },
    {
        "code": "SOC-CHILDCARE",
        "instructions": "Code comments discussing childcare services. Include mentions of high costs, the effectiveness and structure of subsidies, availability of places, quality of care, and the impact on workforce participation (especially for mothers).",
        "name": "Social Services - Childcare Costs and Accessibility"
    },
    {
        "code": "IND-MADE_IN_AUS",
        "instructions": "Code comments expressing a desire for increased domestic manufacturing, national self-sufficiency, support for local businesses, and 'Made in Australia' initiatives.",
        "name": "Industry & Manufacturing - Desire for Domestic Production ('Made in Australia')"
    },
    {
        "code": "IND-RESOURCE_MGT",
        "instructions": "Code comments discussing the management of Australia's natural resources (e.g., minerals, gas). Include mentions of the need for more onshore processing/value-adding, resource taxation/royalties, and concerns about foreign ownership or control of resources.",
        "name": "Industry & Manufacturing - Resource Management, Value-Adding, Foreign Ownership"
    },
    {
        "code": "IND-MANUF_DECLINE",
        "instructions": "Code comments lamenting the decline or perceived hollowing out of the Australian manufacturing sector and the country's dependence on imported goods.",
        "name": "Industry & Manufacturing - Concern over Manufacturing Decline & Reliance on Imports"
    },
    {
        "code": "IND-GOV_INITIATIVES",
        "instructions": "Code comments discussing or analyzing specific government policies and initiatives aimed at boosting industries, such as 'Future Made in Australia', green technology investment, defence industry aspects of AUKUS, or general manufacturing support.",
        "name": "Industry & Manufacturing - Government Initiatives for Industry Development"
    },
    {
        "code": "IND-LIVE_EXPORT_IMPACT",
        "instructions": "Code comments focusing on the economic and social consequences of phasing out the live sheep export trade. Include discussions on the impact on farmers, regional communities, related industries, and adjustment support.",
        "name": "Industry & Manufacturing - Economic and Social Impacts of Live Sheep Export Ban"
    },
    {
        "code": "IND-ENERGY_COST_IMPACT",
        "instructions": "Code comments specifically discussing how energy costs (electricity, gas) and energy policy affect the competitiveness, viability, or operational costs of Australian industry and manufacturing.",
        "name": "Industry & Manufacturing - Impact of Energy Costs on Competitiveness"
    },
    {
        "code": "EDU-DEBT_AFFORD",
        "instructions": "Code comments discussing the cost of tertiary education (university and vocational/TAFE). Include mentions of HECS/HELP debt levels, indexation changes, Fee-Free TAFE initiatives, postgraduate fees, and general affordability concerns.",
        "name": "Education & Skills - Student Debt & Tertiary Affordability"
    },
    {
        "code": "EDU-PUBLIC_SCHOOL_FUND",
        "instructions": "Code comments discussing funding for public schools. Include mentions of adequacy compared to private schools, adherence to Gonski/Schooling Resource Standard (SRS) funding models, resource levels, and equity between sectors.",
        "name": "Education & Skills - Public School Funding & Resources"
    },
    {
        "code": "EDU-TEACHING_CHALLENGES",
        "instructions": "Code comments discussing issues faced by the teaching profession and within schools. Include mentions of teacher shortages, turnover, workload, pay, training quality, and school environment issues like safety, violence, or bullying.",
        "name": "Education & Skills - Challenges in the Teaching Profession & School Environment"
    },
    {
        "code": "EDU-EARLY_CHILDHOOD",
        "instructions": "Code comments discussing early childhood education and care. Include mentions of its importance, cost, accessibility, availability of places, quality standards, and universal access proposals.",
        "name": "Education & Skills - Early Childhood Education Access & Quality"
    },
    {
        "code": "EDU-INTL_STUDENTS",
        "instructions": "Code comments discussing the role of international students in the tertiary education sector. Include mentions of university financial reliance on their fees, impacts on educational standards or resources, campus culture, and links to migration policies.",
        "name": "Education & Skills - Role and Impact of International Students"
    }
]
~~~
# Output instructions

Output a JSON object with a "comments" (DO NOT OUTPUT A BARE LIST) key set to a list of objects with the following keys:
- "id": The comment ID, use the number of the comment in the markdown list of comments.
- "codes": The codes assigned to the comment (e.g., ["ECON-COST_LIVING", "OTHER-DISILLUSION"]) or [] if no codes can be determined.
- "reason": Brief selection of keywords, topic indicators etc, used to identify these codes. Do not just repeat the comment. If no codes are assigned, reason MUST BE an empty string "".

# Comments
```

#### NewsTalk

```
# Instructions
Your task is to analyze the comments for a news story titled '{title}'. The story is related to the 2025 Australian Federal Election. You will code (label) comment topics according the code book we provide. The code book is a list of topics that are likely to be discussed in the context of the election.

Story description: '{description}'

Each code book entry contains the code and instructions which describe when to apply each code. The instructions contain specific examples but are not exhaustive. Use your judgment to apply the codes to the comments based on the instructions. CAREFULLY CONSIDER all potential codes based on these instructions, and include all relevant codes for each comment. 

You will also provide a reason string for code assignment. The reason should be a brief selection of keywords, topic indicators, etc., drawn from the code book (or other factors such as implicit mentions) that were used to identify this topic code. Be terse and not include a whole comment unless it is necessary to illustrate multiple codes, particularly if you assign no topic. An empty string for the reason is acceptable for terse/junk comments with no topics.

## Code book
~~~json
[
    {{
        "code": "OTHER-DISILLUSION",
        "instructions": "Code comments expressing a general lack of faith in major political parties, politicians, or the political system. This includes mentions of career politicians, perceived corruption, politicians being out of touch, and dissatisfaction with the two-party system.",
        "name": "Other - General Political Disillusionment and Cynicism"
    }},
    {{
        "code": "OTHER-MEDIA",
        "instructions": "Code comments discussing the perceived bias of media outlets (e.g., News Corp, ABC) and their influence on political narratives and public opinion. Include discussions about media objectivity and its role in shaping politics.",
        "name": "Other - Media Bias and Influence Concerns"
    }},
    {{
        "code": "OTHER-MINOR_INDP",
        "instructions": "Code comments discussing the role, potential impact, or appeal of minor parties (e.g., Greens, One Nation) and independent candidates (e.g., Teals). Include discussions on strategic voting and the possibility of a hung parliament.",
        "name": "Other - Role and Influence of Minor Parties and Independents"
    }},
    {{
        "code": "OTHER-DUTTON_CRITIQUE",
        "instructions": "Code comments specifically criticizing Opposition Leader Peter Dutton's policies, leadership style, or perceived alignment with Donald Trump's political tactics. Include mentions of specific controversies related to Dutton (e.g., Kirribilli House preference, views on working from home).",
        "name": "Other - Critiques of Peter Dutton and Comparisons to Trump"
    }},
    {{
        "code": "OTHER-DONATIONS_LOBBY",
        "instructions": "Code comments expressing concern about the influence of corporate donations and lobbyists on political decision-making and democratic representation.",
        "name": "Other - Influence of Political Donations and Lobbying"
    }},
    {{
        "code": "OTHER-POLLING",
        "instructions": "Code comments discussing, analyzing, or expressing skepticism about political polls, their methodology, accuracy, or predictive power.",
        "name": "Other - Political Polling Analysis and Skepticism"
    }},
    {{
        "code": "OTHER-OLYMPICS",
        "instructions": "Code comments debating the Brisbane Olympics, focusing on aspects like costs, impact on infrastructure and housing, public sentiment, and perceived wastefulness.",
        "name": "Other - Concerns about the Brisbane Olympics"
    }},
    {{
        "code": "OTHER-TRUTH_ADS",
        "instructions": "Code comments discussing the need for, or debate surrounding, legislation to ensure truthfulness in political advertising.",
        "name": "Other - Debate on Truth in Political Advertising Legislation"
    }},
    {{
        "code": "OTHER-WFH",
        "instructions": "Code comments discussing work-from-home policies, particularly for public servants, often linked to political figures or perceived hypocrisy.",
        "name": "Other - Work From Home Policies (Political Context)"
    }},
    {{
        "code": "OTHER-LIVE_EXPORT_ETHICS",
        "instructions": "Code comments focusing on the ethical and controversial aspects of the live animal export trade, including debates surrounding the proposed ban on live sheep exports.",
        "name": "Other - Live Animal Export Ethical Debate"
    }},
    {{
        "code": "ECON-COST_LIVING",
        "instructions": "Code comments expressing concern about the rising cost of living, focusing on the affordability of essentials like groceries, energy, housing, and transport. Include expressions of economic anxiety and hardship.",
        "name": "Economic Policy - Cost of Living Concerns"
    }},
    {{
        "code": "ECON-TAX",
        "instructions": "Code comments discussing or debating tax policy. This includes income tax cuts, fuel excise changes, bracket creep, fairness of the tax system, and potential taxes on corporations or wealth.",
        "name": "Economic Policy - Tax Policy Debates"
    }},
    {{
        "code": "ECON-SPENDING_DEBT",
        "instructions": "Code comments scrutinizing government spending, fiscal responsibility, national debt levels, the funding and economic impact of infrastructure projects, public service efficiency, and proposed cuts to the public service.",
        "name": "Economic Policy - Government Spending, Debt, and Fiscal Management"
    }},
    {{
        "code": "ECON-ENERGY_POLICY",
        "instructions": "Code comments discussing the economic aspects of energy policy. This includes the cost implications of renewables versus fossil fuels/nuclear, gas reservation policies, and the direct impact of energy policy on household bills and the economy.",
        "name": "Economic Policy - Energy Policy (Economic Aspects)"
    }},
    {{
        "code": "ECON-SUPERMARKET",
        "instructions": "Code comments discussing supermarket pricing, perceived price gouging, market power of the duopoly (Coles/Woolworths), and calls for government regulation or intervention.",
        "name": "Economic Policy - Supermarket Pricing and Regulation"
    }},
    {{
        "code": "ECON-TRANSPORT_AFFORD",
        "instructions": "Code comments discussing the affordability, cost, and investment in public transport, particularly as it relates to cost of living pressures.",
        "name": "Economic Policy - Public Transport Affordability"
    }},
    {{
        "code": "NATSEC-SAFETY_JUSTICE",
        "instructions": "Code comments discussing community safety issues, crime rates (especially youth crime), the effectiveness and conduct of policing, sentencing, bail laws, and perceived problems within the justice system.",
        "name": "National Security - Community Safety, Policing, and Justice"
    }},
    {{
        "code": "NATSEC-RACISM_INTEGRATION",
        "instructions": "Code comments discussing societal attitudes towards immigrants, challenges of integration, cultural clashes, racism, or xenophobia, particularly in the context of immigration debates.",
        "name": "National Security - Racism, Xenophobia, and Immigrant Integration"
    }},
    {{
        "code": "NATSEC-IMMIG_HOUS_INFRA",
        "instructions": "Code comments linking immigration levels (high or low) directly to pressures on housing availability, affordability, infrastructure capacity, and related cost of living impacts, framed as an immigration/population management issue.",
        "name": "National Security - Immigration's Impact on Housing and Infrastructure"
    }},
    {{
        "code": "NATSEC-DEFENCE_FOREIGN",
        "instructions": "Code comments discussing national security strategy, defence spending, military alliances (e.g., AUKUS), relationships with other countries (e.g., US, China), nuclear submarines, foreign espionage, foreign influence, and Australia's strategic positioning.",
        "name": "National Security - Defence Strategy and Foreign Relations"
    }},
    {{
        "code": "NATSEC-BORDER_ASYLUM",
        "instructions": "Code comments discussing border control policies, deportation of non-citizens (especially criminals), management of asylum seekers, boat arrivals, offshore detention, and migration data accuracy.",
        "name": "National Security - Border Security, Deportation, and Asylum Policies"
    }},
    {{
        "code": "HOUS-AFFORD_CRISIS",
        "instructions": "Code comments expressing concern about the high cost and difficulty of affording housing (both buying and renting). Include discussions on the impact on younger generations, intergenerational tension, and feelings of hopelessness regarding home ownership or rental security.",
        "name": "Housing & Infrastructure - Housing Affordability Crisis & Generational Impact"
    }},
    {{
        "code": "HOUS-GOV_POLICY",
        "instructions": "Code comments discussing, debating, or criticizing government housing policies. Include mentions of specific initiatives (e.g., HAFF), tax settings (negative gearing, CGT discounts), foreign investment rules, lending standards, and skepticism about policy effectiveness or political will.",
        "name": "Housing & Infrastructure - Government Housing Policy Critique"
    }},
    {{
        "code": "HOUS-SUPPLY_DEV",
        "instructions": "Code comments focusing on issues related to housing supply. Include discussions on the need for more housing, urban density debates, zoning laws, NIMBYism, construction industry challenges, and land release.",
        "name": "Housing & Infrastructure - Housing Supply, Density & Development Challenges"
    }},
    {{
        "code": "HOUS-INFRA_PROJECTS",
        "instructions": "Code comments discussing general infrastructure issues (e.g., transport reliability, road congestion) or specific large-scale infrastructure projects (e.g., Melbourne Airport Rail Link, T2D tunnel, Brisbane Olympics infrastructure), including their costs, benefits, funding, and planning.",
        "name": "Housing & Infrastructure - Infrastructure Debates and Major Projects"
    }},
    {{
        "code": "HOUS-IMMIG_IMPACT",
        "instructions": "Code comments discussing the role of immigration and population growth as a factor contributing to housing demand, housing shortages, affordability issues, and strain on infrastructure, framed from the perspective of housing outcomes.",
        "name": "Housing & Infrastructure - Immigration's Role in Housing Pressure"
    }},
    {{
        "code": "CLIM-RENEWABLE_IMPL",
        "instructions": "Code comments discussing the practical aspects and challenges of transitioning to renewable energy. Include mentions of grid integration and upgrades, energy storage solutions (e.g., batteries), ensuring reliability, and related infrastructure.",
        "name": "Climate & Energy - Renewable Energy Implementation Challenges"
    }},
    {{
        "code": "CLIM-ENVIRON_CONCERN",
        "instructions": "Code comments expressing broader environmental concerns. Include discussions on habitat destruction, land clearing, mining impacts, pollution, biodiversity loss, species protection, and the need for action to meet climate change targets.",
        "name": "Climate & Energy - Environmental Concerns"
    }},
    {{
        "code": "CLIM-ENERGY_COST",
        "instructions": "Code comments explicitly linking climate and energy policies (renewables, gas, nuclear, subsidies, carbon pricing) to the cost of electricity and gas for households and businesses.",
        "name": "Climate & Energy - Cost of Living & Energy Prices"
    }},
    {{
        "code": "CLIM-NUCLEAR_VS_RENEW",
        "instructions": "Code comments comparing and debating the merits, costs, feasibility, environmental impacts, and timelines of pursuing nuclear power versus accelerating renewable energy development.",
        "name": "Climate & Energy - Nuclear vs. Renewable Energy Debate"
    }},
    {{
        "code": "CLIM-GAS_POLICY",
        "instructions": "Code comments discussing gas policy. Include debates on domestic gas reservation, gas exports, the role of gas as a transition fuel, gas prices, and the environmental impact of gas extraction and use (e.g., fracking).",
        "name": "Climate & Energy - Gas Policy Debate"
    }},
    {{
        "code": "CLIM-EV_FUEL",
        "instructions": "Code comments discussing electric vehicles (EVs), including adoption rates, charging infrastructure, government incentives or taxes (e.g., road user charges), vehicle emission standards, and the future of fuel excise.",
        "name": "Climate & Energy - Electric Vehicles (EVs) & Fuel Policy"
    }},
    {{
        "code": "HEALTH-MEDICARE_AFFORD",
        "instructions": "Code comments discussing Medicare, particularly the availability and decline of bulk billing, out-of-pocket costs for GP and specialist visits, and the general affordability of accessing healthcare services through Medicare.",
        "name": "Healthcare - Medicare, Bulk Billing & Affordability"
    }},
    {{
        "code": "HEALTH-HOSPITALS",
        "instructions": "Code comments discussing issues within the hospital system. Include mentions of overcrowded emergency departments (EDs), long wait times for appointments or surgeries, ambulance ramping, urgent care clinics, and perceived inadequacy of hospital resources or funding.",
        "name": "Healthcare - Hospital System Strain & Access"
    }},
    {{
        "code": "HEALTH-MENTAL",
        "instructions": "Code comments discussing the accessibility and affordability of mental health services. Include mentions of difficulty finding practitioners, cost barriers, long wait times, insufficient Medicare rebates, and the need for better funding or integration.",
        "name": "Healthcare - Mental Healthcare Access & Funding"
    }},
    {{
        "code": "HEALTH-DENTAL",
        "instructions": "Code comments discussing the high cost and difficulty of accessing dental care. Include calls for dental services to be included under Medicare and mentions of people avoiding or delaying treatment due to cost.",
        "name": "Healthcare - Dental Care Access & Medicare Inclusion"
    }},
    {{
        "code": "HEALTH-VAX_PUBLIC",
        "instructions": "Code comments discussing public health matters such as vaccination rates, management of infectious diseases (e.g., measles, flu), vaccine hesitancy, public health campaigns, and related government policies.",
        "name": "Healthcare - Vaccination & Public Health Issues"
    }},
    {{
        "code": "HEALTH-SMOKE_VAPE",
        "instructions": "Code comments discussing government policies related to tobacco and vaping. Include mentions of taxes, regulations, availability, black markets, and public health consequences or debates.",
        "name": "Healthcare - Smoking/Vaping Policy & Health Impacts"
    }},
    {{
        "code": "HEALTH-WOMENS",
        "instructions": "Code comments discussing specific health issues and services relevant to women. Include mentions of funding for specific conditions (e.g., endometriosis), access to reproductive healthcare (e.g., abortion), maternity services, and perceived historical underfunding.",
        "name": "Healthcare - Women's Health Initiatives & Access"
    }},
    {{
        "code": "SOC-WELFARE_PAYMENTS",
        "instructions": "Code comments discussing the adequacy of social welfare payments like JobSeeker, pensions, or other Centrelink benefits. Include mentions of payment rates relative to poverty levels, eligibility criteria, partner income tests, and difficulties dealing with Centrelink/Services Australia.",
        "name": "Social Services - Welfare Payment Adequacy and Access"
    }},
    {{
        "code": "SOC-NDIS",
        "instructions": "Code comments discussing the National Disability Insurance Scheme (NDIS). Include mentions of its sustainability, management, rising costs, potential misuse or rorting, and calls for reform, while acknowledging its necessity.",
        "name": "Social Services - NDIS Concerns (Cost, Rorting, Reform)"
    }},
    {{
        "code": "SOC-HOMELESSNESS",
        "instructions": "Code comments discussing homelessness and the challenges faced by homeless individuals in accessing shelter, support services, and the adequacy of the social safety net.",
        "name": "Social Services - Homelessness and Housing Support Challenges"
    }},
    {{
        "code": "SOC-PUBLIC_SVC_DELIVERY",
        "instructions": "Code comments expressing concern about the public service workforce, potential job cuts, and the resulting impact on the quality, timeliness, or accessibility of government services (e.g., Centrelink, Medicare, DVA).",
        "name": "Social Services - Public Sector Workforce and Service Delivery Concerns"
    }},
    {{
        "code": "SOC-WORKER_RIGHTS",
        "instructions": "Code comments discussing employment conditions and protections. Include mentions of fair wages, wage theft, casualization, insecure work, the role and importance of unions, and Fair Work regulations.",
        "name": "Social Services - Worker Rights, Wages, and Union Role"
    }},
    {{
        "code": "SOC-CHILDCARE",
        "instructions": "Code comments discussing childcare services. Include mentions of high costs, the effectiveness and structure of subsidies, availability of places, quality of care, and the impact on workforce participation (especially for mothers).",
        "name": "Social Services - Childcare Costs and Accessibility"
    }},
    {{
        "code": "IND-MADE_IN_AUS",
        "instructions": "Code comments expressing a desire for increased domestic manufacturing, national self-sufficiency, support for local businesses, and 'Made in Australia' initiatives.",
        "name": "Industry & Manufacturing - Desire for Domestic Production ('Made in Australia')"
    }},
    {{
        "code": "IND-RESOURCE_MGT",
        "instructions": "Code comments discussing the management of Australia's natural resources (e.g., minerals, gas). Include mentions of the need for more onshore processing/value-adding, resource taxation/royalties, and concerns about foreign ownership or control of resources.",
        "name": "Industry & Manufacturing - Resource Management, Value-Adding, Foreign Ownership"
    }},
    {{
        "code": "IND-MANUF_DECLINE",
        "instructions": "Code comments lamenting the decline or perceived hollowing out of the Australian manufacturing sector and the country's dependence on imported goods.",
        "name": "Industry & Manufacturing - Concern over Manufacturing Decline & Reliance on Imports"
    }},
    {{
        "code": "IND-GOV_INITIATIVES",
        "instructions": "Code comments discussing or analyzing specific government policies and initiatives aimed at boosting industries, such as 'Future Made in Australia', green technology investment, defence industry aspects of AUKUS, or general manufacturing support.",
        "name": "Industry & Manufacturing - Government Initiatives for Industry Development"
    }},
    {{
        "code": "IND-LIVE_EXPORT_IMPACT",
        "instructions": "Code comments focusing on the economic and social consequences of phasing out the live sheep export trade. Include discussions on the impact on farmers, regional communities, related industries, and adjustment support.",
        "name": "Industry & Manufacturing - Economic and Social Impacts of Live Sheep Export Ban"
    }},
    {{
        "code": "IND-ENERGY_COST_IMPACT",
        "instructions": "Code comments specifically discussing how energy costs (electricity, gas) and energy policy affect the competitiveness, viability, or operational costs of Australian industry and manufacturing.",
        "name": "Industry & Manufacturing - Impact of Energy Costs on Competitiveness"
    }},
    {{
        "code": "EDU-DEBT_AFFORD",
        "instructions": "Code comments discussing the cost of tertiary education (university and vocational/TAFE). Include mentions of HECS/HELP debt levels, indexation changes, Fee-Free TAFE initiatives, postgraduate fees, and general affordability concerns.",
        "name": "Education & Skills - Student Debt & Tertiary Affordability"
    }},
    {{
        "code": "EDU-PUBLIC_SCHOOL_FUND",
        "instructions": "Code comments discussing funding for public schools. Include mentions of adequacy compared to private schools, adherence to Gonski/Schooling Resource Standard (SRS) funding models, resource levels, and equity between sectors.",
        "name": "Education & Skills - Public School Funding & Resources"
    }},
    {{
        "code": "EDU-TEACHING_CHALLENGES",
        "instructions": "Code comments discussing issues faced by the teaching profession and within schools. Include mentions of teacher shortages, turnover, workload, pay, training quality, and school environment issues like safety, violence, or bullying.",
        "name": "Education & Skills - Challenges in the Teaching Profession & School Environment"
    }},
    {{
        "code": "EDU-EARLY_CHILDHOOD",
        "instructions": "Code comments discussing early childhood education and care. Include mentions of its importance, cost, accessibility, availability of places, quality standards, and universal access proposals.",
        "name": "Education & Skills - Early Childhood Education Access & Quality"
    }},
    {{
        "code": "EDU-INTL_STUDENTS",
        "instructions": "Code comments discussing the role of international students in the tertiary education sector. Include mentions of university financial reliance on their fees, impacts on educational standards or resources, campus culture, and links to migration policies.",
        "name": "Education & Skills - Role and Impact of International Students"
    }}
]
~~~

## Threaded structure, replies and ambiguity

The comments are provided in a nested structure, with each comment numbered (the comment id). The nested comments are indented to show the hierarchy. This can be useful for understanding the context beyond the text of the comment itself. For example, if a comment makes clearly identifies a topic, and a reply (indented) comment doesn't mention the topic but appears to engage in the topic then you must code the reply with the same topic(s) as the comment it is replying to. You should also differentiate which topics a reply is engaging with. In the case of ambiguity, then assign no topics. For example if a comment refers to multiple topics and a reply simply says "I agree", we cannot tell which topic the comment is agreeing with and no topic should be assigned to the reply. However if the reply says "I agree with your point about the cost of living" then you can assign the cost of living topic to the reply.

Top level comments often replying to the news story, or the authors. You may need to consider the topics of the news story to assign codes to these comments.

# Output instructions

Output a JSON object with a "comments" key set to a list of objects with the following keys:
- "id": The comment ID, use the number of the comment in the markdown list of comments.
- "codes": The codes assigned to the comment (e.g., ["ECON-COST_LIVING", "OTHER-DISILLUSION"]) or [] if no codes can be determined.
- "reason": Brief selection of keywords, topic indicators etc, used to identify these codes. Do not just repeat the comment. Be terse.

# Comments to code
{comments}
```

### Political Support Coding

These prompts code comments based on the political support expressed in the comment. The codes are designed to capture the sentiment towards major political parties, leaders, and specific policies. 

#### Reddit

```
 Instructions
Your task is to analyze the comments for a Reddit submission related to the 2025 Australian Federal Election and infer political party support or opposition for each.

## Political Parties

- ALP (Australian Labor Party) - Leader: Anthony Albanese
- LNP (Liberal National Party Coalition) - Leader: Peter Dutton (This also means Nationals)
- GREENS (Australian Greens) - Leader: Adam Bandt
- OTHER (Other parties or independents)
- NONE (No party identified)
- ALL (All parties)

## Summary of manifestos 

This section summarizes the key points from the manifestos of the major parties in the 2025 Australian Federal Election.

### Economic Policy

Tax Reform: Tax cuts (ALP, LNP), review of government spending (LNP), cutting government waste (LNP)
Cost of Living Support: Energy bill relief (ALP), lowering inflation (LNP)
Economic Regulation: Penalizing anti-competitive behaviors (LNP), breaking up supermarket duopoly (GREENS)

ALP proposes 20% student debt reduction
LNP aim to remove regulatory roadblocks
Greens advocate for 50-cent public transport
LNP strategy is to infer weakness in the economy is Labors fault and the economy will be stonger under them

### Healthcare

Medicare Enhancement: Bulk billing improvements (ALL), increased GP numbers (ALL)
Specialized Health Services: Urgent care clinics (ALP), mental health support (LNP, GREENS)
Women's Health: Endometriosis/pelvic pain clinics (ALP), ovarian cancer initiatives (LNP)

Labor plans to establish urgent care clinics
LNP committing $9 billion for bulk billing
Greens want to include dental care in Medicare

### Housing & Infrastructure

Housing Affordability: Building 1.2 million homes (ALP), affordable housing initiatives (LNP)
Foreign Investment: 2-year ban on foreign investment in existing homes (ALP, LNP)
Rental Market: Rent increase limitations (GREENS), national renters protection authority (GREENS)

ALP proposes "Help to Buy" scheme
LNP aim to reduce migration to address housing pressure
Greens advocate for government-owned housing developer

### Climate & Energy

Renewable Energy: 82% renewables in grid target (ALP), $8 billion investment in renewables (ALP)
Energy Sources: Nuclear energy (LNP), gas reserve (LNP), public-owned renewables (GREENS)
Environmental Protection: End to native logging (GREENS), conservation of native species (GREENS)

ALP investing $8 billion in renewables and low emissions, promises subsidity of home batteries
LNP support nuclear energy development, removing vehicle fuel efficiency measures
Greens oppose all new coal and gas projects

### Education & Skills

Tertiary Education: Free TAFE (ALP, GREENS), student debt reduction (ALP) or elimination (GREENS)
Early Education: Improved access to early education (ALP), free universal childcare (GREENS)
School Support: Free public schools (GREENS), back-to-school payments (GREENS)

ALP focuses on early education access
Greens propose wiping all student debt
Labor mentions Free TAFE multiple times in platform

### Social Services

Welfare Support: Increased Centrelink payments (GREENS), pension raises (GREENS)
Child Care: Childcare assistance (ALP), free universal childcare (GREENS)
Worker Rights: Employment protections (GREENS)

Greens advocate lowering retirement age
Labor emphasizes childcare affordability
Greens propose free universal childcare

### National Security & Immigration

Border Security: Deportation of criminals (LNP), stopping people smugglers (LNP)
Migration Policy: Reduced migration (LNP), reduced foreign students at metropolitan universities (LNP)
Community Safety: Strengthened bail laws (LNP), addressing youth knife crime (LNP)

LNP focus on strengthening bail laws
LNP propose making posting videos of crimes for notoriety illegal for youth
LNP aim to reduce the refugee/humanitarian program
LNP reducing cap on foreign students, and lowering of general immigration quota

### Industry & Manufacturing

Australian Production: "Made in Australia" initiatives (ALP), support for buying Australian (ALP)
Industry Transition: Job creation during climate transition (GREENS)

ALP emphasizes "Building a future made in Australia"
Labor promotes support for buying Australian products

## Instructions

1. Identify the MAIN political party being discussed.
- Use explicit mentions of party names, leaders, or common nicknames to identify the party
- Identify parties based on specific policies or ideologies strongly associated with each party
- If the comment  refers to both major parties, or all parties, e.g. politicians, parties and politics in general, use the "ALL" code
- If no party can be identified, set Party to ""

2. Determine the sentiment (Support or Opposition) towards the identified party(ies)
- Sentiment refers to the commenter's attitude towards the identified party
- Party support can be inferred by the commenter aligning with key party policies
- If no specific party is identifiable, set sentiment to "NONE"
- If no sentiment (or neutral sentiment) towards the mentioned party can be determined, set sentiment to "NONE"

4. Provide a concise explanation for your party and sentiment classifications
- Set the "reason" field to explain your classification, briefly explaining the keywords, phrases, or sentiment indicators that led to those inferences
- If no party OR no sentiment can be determined, then mention that in the reason field, e.g. "LNP because Peter Dutton was mentioned, but no sentiment", "No party and no sentiment was identified".

## Output instructions

Output JSON with a "comments" key set to a list of objects with the following keys and values:
- "id": The comment ID.
- "party": The main poltical party, one of ALP, LNP, GREENS, OTHER, ALL, or NONE if no party identified. Do not use any other codes! Reminder, Nationals should be coded as LNP.
- "sentiment": The sentiment towards the identified party ("SUPPORT", "OPPOSE", or "NONE").
- "reason": A brief explanation for the party and sentiment reasoning.

# Comments
```

#### NewsTalk

```
# Instructions
Your task is to analyze comments posted on a news web site for a story titled '{title}'. The story is related to the 2025 Australian Federal Election. You will infer political party support or opposition for each comment where possible. A description of the story is '{description}'.

## Political Parties

- ALP (Australian Labor Party) - Leader: Anthony Albanese
- LNP (Liberal National Party Coalition) - Leader: Peter Dutton (This also means Nationals)
- GREENS (Australian Greens) - Leader: Adam Bandt
- OTHER (Other parties or independents)
- NONE (No party identified)
- ALL (All parties)

## Summary of manifestos 

This section summarizes the key points from the manifestos of the major parties in the 2025 Australian Federal Election.

### Economic Policy

Tax Reform: Tax cuts (ALP, LNP), review of government spending (LNP), cutting government waste (LNP)
Cost of Living Support: Energy bill relief (ALP), lowering inflation (LNP)
Economic Regulation: Penalizing anti-competitive behaviors (LNP), breaking up supermarket duopoly (GREENS)

ALP proposes 20% student debt reduction
LNP aim to remove regulatory roadblocks
Greens advocate for 50-cent public transport
LNP strategy is to infer weakness in the economy is Labors fault and the economy will be stonger under them

### Healthcare

Medicare Enhancement: Bulk billing improvements (ALL), increased GP numbers (ALL)
Specialized Health Services: Urgent care clinics (ALP), mental health support (LNP, GREENS)
Women's Health: Endometriosis/pelvic pain clinics (ALP), ovarian cancer initiatives (LNP)

Labor plans to establish urgent care clinics
LNP committing $9 billion for bulk billing
Greens want to include dental care in Medicare

### Housing & Infrastructure

Housing Affordability: Building 1.2 million homes (ALP), affordable housing initiatives (LNP)
Foreign Investment: 2-year ban on foreign investment in existing homes (ALP, LNP)
Rental Market: Rent increase limitations (GREENS), national renters protection authority (GREENS)

ALP proposes "Help to Buy" scheme
LNP aim to reduce migration to address housing pressure
Greens advocate for government-owned housing developer

### Climate & Energy

Renewable Energy: 82% renewables in grid target (ALP), $8 billion investment in renewables (ALP)
Energy Sources: Nuclear energy (LNP), gas reserve (LNP), public-owned renewables (GREENS)
Environmental Protection: End to native logging (GREENS), conservation of native species (GREENS)

ALP investing $8 billion in renewables and low emissions, promises subsidity of home batteries
LNP support nuclear energy development, removing vehicle fuel efficiency measures
Greens oppose all new coal and gas projects

### Education & Skills

Tertiary Education: Free TAFE (ALP, GREENS), student debt reduction (ALP) or elimination (GREENS)
Early Education: Improved access to early education (ALP), free universal childcare (GREENS)
School Support: Free public schools (GREENS), back-to-school payments (GREENS)

ALP focuses on early education access
Greens propose wiping all student debt
Labor mentions Free TAFE multiple times in platform

### Social Services

Welfare Support: Increased Centrelink payments (GREENS), pension raises (GREENS)
Child Care: Childcare assistance (ALP), free universal childcare (GREENS)
Worker Rights: Employment protections (GREENS)

Greens advocate lowering retirement age
Labor emphasizes childcare affordability
Greens propose free universal childcare

### National Security & Immigration

Border Security: Deportation of criminals (LNP), stopping people smugglers (LNP)
Migration Policy: Reduced migration (LNP), reduced foreign students at metropolitan universities (LNP)
Community Safety: Strengthened bail laws (LNP), addressing youth knife crime (LNP)

LNP focus on strengthening bail laws
LNP propose making posting videos of crimes for notoriety illegal for youth
LNP aim to reduce the refugee/humanitarian program
LNP reducing cap on foreign students, and lowering of general immigration quota

### Industry & Manufacturing

Australian Production: "Made in Australia" initiatives (ALP), support for buying Australian (ALP)
Industry Transition: Job creation during climate transition (GREENS)

ALP emphasizes "Building a future made in Australia"
Labor promotes support for buying Australian products

## Instructions

You are provided a nested representation of a a comment thread. Each comment is numbered (the comment id). The nested comments are indented to show the hierarchy, and this MAY be useful for understanding the context beyond the text of the comment itself. For example, if a comment makes an a reference to a party (with support or opposition), and a reply doesn't mention the party but clearly suggests support or opposition, you should code the reply with the same party and sentiment as the comment it is replying to.

1. Identify the MAIN political party being discussed.
- Use explicit mentions of party names, leaders, or common nicknames to identify the party
- Identify parties based on specific policies or ideologies strongly associated with each party
- If the comment  refers to both major parties, or all parties, e.g. politicians, parties and politics in general, use the "ALL" code
- If no party can be identified, set Party to ""

2. Determine the sentiment (Support or Opposition) towards the identified party(ies)
- Sentiment refers to the commenter's attitude towards the identified party
- Party support can be inferred by the commenter aligning with key party policies
- If no specific party is identifiable, set sentiment to "NONE"
- If no sentiment (or neutral sentiment) towards the mentioned party can be determined, set sentiment to "NONE"

4. Provide a concise explanation for your party and sentiment classifications
- Set the "reason" field to explain your classification, briefly explaining the keywords, phrases, or sentiment indicators that led to those inferences. This must be terse.
- If no party OR no sentiment can be determined, then mention that in the reason field, e.g. "LNP because Peter Dutton was mentioned, but no sentiment", "No party and no sentiment was identified".

## Output instructions

Output JSON with a "comments" key set to a list of objects with the following keys and values:
- "id": The comment ID.
- "party": The main poltical party, one of ALP, LNP, GREENS, OTHER, ALL, or NONE if no party identified. Do not use any other codes! Reminder, Nationals should be coded as LNP.
- "sentiment": The sentiment towards the identified party ("SUPPORT", "OPPOSE", or "NONE").
- "reason": A terse explanation for your coding of party and sentiment in light of the instructions.

# Comments to code
{comments}
````
