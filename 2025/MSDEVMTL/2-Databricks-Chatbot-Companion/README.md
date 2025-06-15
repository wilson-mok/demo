# Azure Databricks - Building a Chatbot Companion with AI/BI Genie

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo include:

– Introduction to AI/BI Genie.
– Customization techniques to tailor Genie's response to deliver consistent and trustworthy insights.
– Applying the best practices and techniques to create our own Genie.
- Develop a Streamlit app using Databricks' Genie API.

After this session, you will be able to customize and optimize your own data companion. 

## Presentation
* Presentation: [Azure Databricks - Building a Chatbot Companion with AI/BI Genie](Azure%20Databricks%20-%20Building%20a%20Chatbot%20Companion%20with%20AIBI%20Genie.pdf)

## Setup
### Pre-req

For this demo, we require:

1. Azure Databricks with Unity Catalog enabled.
1. A Catalog named 'demo_catalog' has been created.
1. A Schema named 'demo_yelp_academic' has been created under 'demo_catalog'.
1. A Managed Volume named 'raw_data' has been created under the 'demo_catalog'.

## Code and scripts

Below contains the scripts and configuration details for the Genie. The creation of the Genie will have to be manual. Please follow along using the video recording.

Note: Yelp might provide data from your city instead of Edmonton. If any Genie configuration/test cases refer to Edmonton or Alberta, please substitute it with your location.

### Data for Genie

| Src filename | Description |
| --- | --- |
| [1-Create Table.py](./src/1-Create%20Table.py) | Pre-req: The Yelp JSON files uploaded are uploaded to the 'raw_data' managed volume. This script will read the business, checkin, review, tip, and user JSONs and create the bronze datasets. |
| [2-Transform Data.py](./src/2-Transform%20Data.py) | This script extracts and flattens the data from the JSON to create the Silver tables. |
| [3-Metadata and relationships.py](./src/3-Metadata%20and%20relationships.py) | This script will apply the metadata to the Silver tables. This includes table description, column comments and PK/FK. |

| Data Folder/file | Description |
| --- | --- |
| [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/) | This file contains the Yelp data for the Genie demo. Download the JSON and upload the JSON files to Databricks using [File Upload UI](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-data?wt.mc_id=MVP_365600) to the 'raw_data' managed volume. |

### Streamlit App
In thie demo, we created a custom Streamlit application using the Databricks' Genie API/SDK. Some key features are:
- Start a conversation and ask follow-up questions.
- Display the Genie's text reply
- Display the business location, tip and text on a map instead of in the chat window.
- Optional: Display the generated SQL in the chat window. Currently, this is commented out. 

To run the Streamlit Genie app: 

1. Pre-req: Install your favour IDE (I use VSCode) and have Python installed. Recommended to setup a Virtual Python environment. 
1. Download the source code in the [src/app folder](./src/app/).
1. Install the dependencies `pip -install -r requirements.txt`
1. Create a `.env_local` file using the `.env_sample`. Update the value based on your Databricks environment.
1. Run the Streamlit app by `streamlit run genie_app.py`


### Genie configuration
* Description
  ```
    This Genie is powered by Yelp's enriched dataset, including businesses, users, reviews, and tips — all normalized and enhanced with AI-generated insights.

    It allows users to explore business quality, customer sentiment, and user engagement through natural language queries. Behind the scenes, AI functions extract sentiment, summarize reviews, and identify key topics from customer feedback.

    Use this Genie to:
    - Analyze customer opinions across thousands of reviews
    - Surface top-performing businesses by sentiment and feedback
    - Discover popular tips and elite users with meaningful impact
    - Understand category trends and experience quality by location

    Target Audience: Business users, SMEs, analysts and decision-makers.
  ```

* Sample questions
   * What are the top 5 most-reviewed restaurants in Edmonton, Alberta?
   * Which business categories in Edmonton have the highest average star rating?
   * How did the number of reviews change month-to-month in Edmonton for the last 12 months?
   * What are the top 5 restaurants in Edmonton?
   * What are the most highly rated restaurants in Edmonton based on elite user reviews?

* Data
  * silver_business
  * silver_review
  * silver_tip
  * silver_user

* General Instructions

    ```md
    ## Tone & Format
    - Use a professional but conversational tone.
    - When a business or business name is returned, must include the location details.
    - The location should be provided in terms of latitude and longitude.

    ## Business Focus & Priorities
    - When filtering for **elite user**, only include reviews written by users where: `elite_year_count > 0`
    - When the user asks for “top”, “top-rated”, “best”, or “highest rated” businesses, interpret all these as equivalent and use the **Top Business**

    ## Trusted Data & Logic
    - When asked to summarize review text or tip text, use SQL function ai_summarize with parameter max_words=25.

    ## Limitations & Guardrails
    - When referencing data with 'date' use Jan 19, 2022 as the current date.
    - Do not guess on incomplete data—return “I don’t have enough data” instead.
    - If the response involves summarizing business reviews or tips, only summarize up to a maximum of 10 records. Avoid aggregating more than 10 at a time to ensure clarity and readability.


    ## Fun
    - When asked about businesses in Neverland, generate a witty response and show the following image: https://resizing.flixster.com/JJjdZ-l7PXkQ2xbcWG1gb1dMKVo=/206x305/v2/https://resizing.flixster.com/-XZAfHZM39UwaGJIFWKAE8fS0ak=/v3/t/assets/p29129_p_v8_ak.jpg
     ```

* SQL Queries
    * Query name: Business in City with location
        * Parameter 1: city. Default: Edmonton
        * Query:

            ```sql
            SELECT
                b.`business_id`,
                b.`name`,
                b.`latitude`,
                b.`longitude`,
                b.`city`,
                b.`state`
            FROM 
                `demo_catalog`.`demo_yelp_academic`.`silver_business` b
            WHERE 
                b.`city` = :city
            ```
    * Query name: Latest Business Tip Summary
        * Parameter 1: business_id. Default: wUZXm4KN2wIqlfkvXVZXUw
        * Query:

            ```sql
            with cte AS (
                SELECT
                    t.`user_id`,
                    t.`business_id`,
                    t.`tip_date`,
                    t.`text`,
                    ROW_NUMBER() OVER (ORDER BY t.`tip_date` DESC) AS RANK
                FROM 
                    `demo_catalog`.`demo_yelp_academic`.`silver_tip` t
                WHERE
                    t.`business_id`= :business_id
            )
            SELECT 
                `user_id`,
                `business_id`,
                `tip_date`,
                ai_summarize(`text`,10) as tip_text
            FROM cte
            WHERE rank < 10
            ```
    * Query name: Latest Business Review Summary
        * Parameter 1: business_id. Default: XQfwVwDr-v0ZS3_CbbE5Xw
        * Query:

            ```sql
            with cte AS (
                SELECT
                    r.`review_id`,
                    r.`user_id`,
                    r.`business_id`,
                    r.`review_date`,
                    r.`text`,
                    ROW_NUMBER() OVER (ORDER BY r.`review_date` DESC) AS RANK
                FROM 
                    `demo_catalog`.`demo_yelp_academic`.`silver_review` r
                WHERE
                    r.`business_id`= :business_id
            )
            SELECT 
                `user_id`,
                `business_id`,
                `review_date`,
                ai_summarize(`text`,10) as review_text
            FROM cte
            WHERE rank < 10

            ```


* Benchmarks

    * Top/Highest rated/top-rated

        * Question 1:  What are the top 5 businesses in Edmonton?
        * Question 2:  Which businesses are the highest rated in Edmonton?
        * Query:

            ```sql
            WITH ranked_businesses AS (
                SELECT
                    `business_id`,
                    `name`,
                    `latitude`,
                    `longitude`,
                    `stars`,
                    ROW_NUMBER() OVER (ORDER BY `stars` DESC) AS rank
                FROM
                    `demo_catalog`.`demo_yelp_academic`.`silver_business`
                WHERE
                    `city` = 'Edmonton'
            )
            SELECT
                `business_id`,
                `name`,
                `latitude`,
                `longitude`,
                `stars`
            FROM
                ranked_businesses
            WHERE
                rank <= 5
            ORDER BY
                rank
        ```

    * Number of Reviews in the last 12 months
        * Question 1: Show me the monthly number of reviews for the past 12 months. Show me the year, month and review count
        * Question 2: How many reviews did we get each month over the last year? I need to see the year, month and count
        * Query:

            ```sql
            SELECT
                YEAR(`review_date`) AS `year`,
                MONTH(`review_date`) AS `month`,
                COUNT(*) AS `review_count`
            FROM
                `demo_catalog`.`demo_yelp_academic`.`silver_review`
            JOIN `demo_catalog`.`demo_yelp_academic`.`silver_business`
                ON `silver_review`.`business_id` = `silver_business`.`business_id`
            WHERE
                `review_date` >= ADD_MONTHS(DATE '2022-01-19', -12)
            GROUP BY
                YEAR(`review_date`),
                MONTH(`review_date`)
            ORDER BY
                YEAR(`review_date`),
                MONTH(`review_date`)
            ```
    * Top 5 Businesses with Most Reviews 
        * Question 1: What are the top 5 most-reviewed businesses?
        * Question 2: Show me the top 5 businesses with the most customer reviews.
        * Query:
            
            ```sql
            WITH ranked_businesses AS (
                SELECT
                    `silver_business`.`business_id`,
                    `silver_business`.`name`,
                    `silver_business`.`review_count`,
                    ROW_NUMBER() OVER (ORDER BY `silver_business`.`review_count` DESC) AS rank
                FROM
                    `demo_catalog`.`demo_yelp_academic`.`silver_business`
            )
            SELECT
                `business_id`,
                `name`,
                `review_count`
            FROM
                ranked_businesses
            WHERE
                rank <= 5
            ORDER BY
                rank
            ```
    

