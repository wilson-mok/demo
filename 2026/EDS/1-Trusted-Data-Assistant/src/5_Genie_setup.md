# Genie Setup

## Data
* silver_business
* silver_review
* silver_tip
* silver_user

## Instructions
### General Instructions
```
## Tone & Format
- Use a professional but conversational tone.
- When a business or business name is returned, must include the location details.
- The location should be provided in terms of latitude and longitude.

## Business Focus & Priorities
- When filtering for **elite user**, only include reviews written by users where: `elite_year_count > 0`
- When the user asks for “top”, “top-rated”, “best”, or “highest rated” businesses, interpret all these as equivalent and use the **Top Business**
- **Top Business** means ranking the data over highest stars and highest review_count

## Trusted Data & Logic
- When asked to summarize review text or tip text, use SQL function ai_summarize with parameter max_words=25.

## Limitations & Guardrails
- When referencing data with 'date' use Jan 19, 2022 as the current date.
- Do not guess on incomplete data—return “I don't have enough data” instead.
- If the response involves summarizing business reviews or tips, only summarize up to a maximum of 10 records. Avoid aggregating more than 10 at a time to ensure clarity and readability.


## Fun
- When asked about businesses in Neverland, generate a witty response and show the following image: https://resizing.flixster.com/JJjdZ-l7PXkQ2xbcWG1gb1dMKVo=/206x305/v2/https://resizing.flixster.com/-XZAfHZM39UwaGJIFWKAE8fS0ak=/v3/t/assets/p29129_p_v8_ak.jpg
```

### SQL Queries
1. Business in City with location
    * Query
        ``` sql
        SELECT
            b.`business_id`,
            b.`name`,
            b.`latitude`,
            b.`longitude`,
            b.`city`,
            b.`state`
        FROM 
            `demo_catalog`.`demo_yelp_academic`.`zz_silver_business` b
        WHERE 
            b.`city` = :city
        ```
    * Parameters
        * city = Edmonton
1. Latest Business Tip Summary
    * Query
        ``` sql
        with cte AS (
            SELECT
                t.`user_id`,
                t.`business_id`,
                t.`tip_date`,
                t.`text`,
                ROW_NUMBER() OVER (ORDER BY t.`tip_date` DESC) AS RANK
            FROM 
                `demo_catalog`.`demo_yelp_academic`.`zz_silver_tip` t
            WHERE
                t.`business_id`= :business_id
        )
        SELECT 
            `user_id`,
            `business_id`,
            `tip_date`,
            ai_summarize(`text`,25) as tip_text
        FROM cte
        WHERE rank < 10
        ```
    * Parameters
        * business_id = wUZXm4KN2wIqlfkvXVZXUw
    * Usage Guidance
        * Use this query when the silver_tip's text needs to be summarized
1. Latest Business Review Summary
    * Query
        ``` sql
        with cte AS (
            SELECT
                r.`review_id`,
                r.`user_id`,
                r.`business_id`,
                r.`review_date`,
                r.`text`,
                ROW_NUMBER() OVER (ORDER BY r.`review_date` DESC) AS RANK
            FROM 
                `demo_catalog`.`demo_yelp_academic`.`zz_silver_review` r
            WHERE
                r.`business_id`= :business_id
        )
        SELECT 
            `user_id`,
            `business_id`,
            `review_date`,
            ai_summarize(`text`,25) as review_text
        FROM cte
        WHERE rank < 10
        ```
    * Parameters
        * business_id = XQfwVwDr-v0ZS3_CbbE5Xw
    * Usage Guidance
        * Use this query when the silver_review's text needs to be summarized

## Settings
* Title: Custom Yelp Genie
* Description: 
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
    * In Edmonton, AB, “Duchess Bake Shop” is one of the highest-rated restaurants. Can you summarize the top 5 reviews?

# Benchmarks
1. What are the top 5 businesses in Edmonton?
    ``` sql
    WITH ranked_businesses AS (
        SELECT
            b.`business_id`,
            b.`name`,
            b.`latitude`,
            b.`longitude`,
            b.`city`,
            b.`state`,
            b.`stars`,
            b.`review_count`,
            RANK() OVER (ORDER BY b.`stars` DESC, b.`review_count` DESC) AS rank
        FROM
            `demo_catalog`.`demo_yelp_academic`.`silver_business` b
        WHERE
            b.`city` = 'Edmonton'
            AND b.`stars` IS NOT NULL
    )
    SELECT
        business_id,
        name,
        latitude,
        longitude,
        city,
        state,
        stars,
        review_count
    FROM
        ranked_businesses
    WHERE
        rank <= 5
    ```
1. Which businesses are the highest rated in Edmonton?
    ``` sql
    WITH ranked_businesses AS (
        SELECT
            b.`business_id`,
            b.`name`,
            b.`latitude`,
            b.`longitude`,
            b.`city`,
            b.`state`,
            b.`stars`,
            b.`review_count`,
            RANK() OVER (ORDER BY b.`stars` DESC, b.`review_count` DESC) AS rank
        FROM
            `demo_catalog`.`demo_yelp_academic`.`silver_business` b
        WHERE
            b.`city` = 'Edmonton'
            AND b.`stars` IS NOT NULL
    )
    SELECT
        business_id,
        name,
        latitude,
        longitude,
        city,
        state,
        stars,
        review_count
    FROM
        ranked_businesses
    WHERE
        rank <= 5
    ```
1. Show me the monthly number of reviews for the past 12 months. Show me the year, month and review count
    ``` sql
    SELECT
        YEAR(`review_date`) AS `year`,
        MONTH(`review_date`) AS `month`,
        COUNT(*) AS `review_count`
    FROM
        `demo_catalog`.`demo_yelp_academic`.`silver_review`
            JOIN `demo_catalog`.`demo_yelp_academic`.`silver_business`
            ON `silver_review`.`business_id` = `silver_business`.`business_id`
    WHERE
        `review_date` >= DATE_TRUNC('MONTH', DATEADD(MONTH, -12, DATE('2022-01-19')))
    GROUP BY
        YEAR(`review_date`),
        MONTH(`review_date`)
    ORDER BY
        YEAR(`review_date`),
        MONTH(`review_date`)
    ```
1. How many reviews did we get each month over the last year? I need to see the year (int), month (int) and count
    ``` sql
    SELECT
        YEAR(`review_date`) AS `year`,
        MONTH(`review_date`) AS `month`,
        COUNT(*) AS `review_count`
    FROM
        `demo_catalog`.`demo_yelp_academic`.`silver_review`
            JOIN `demo_catalog`.`demo_yelp_academic`.`silver_business`
            ON `silver_review`.`business_id` = `silver_business`.`business_id`
    WHERE
        `review_date` >= DATE_TRUNC('MONTH', DATEADD(MONTH, -12, DATE('2022-01-19')))
    GROUP BY
        YEAR(`review_date`),
        MONTH(`review_date`)
    ORDER BY
        YEAR(`review_date`),
        MONTH(`review_date`)
    ```
1. What are the top 5 most-reviewed businesses?
    ``` sql
    SELECT
        `business_id`,
        `name`,
        `latitude`,
        `longitude`,
        `city`,
        `state`,
        `review_count`
    FROM
        `demo_catalog`.`demo_yelp_academic`.`zz_silver_business`
    WHERE
        `review_count` IS NOT NULL
    ORDER BY
        `review_count` DESC
    LIMIT 5
    ```
1. Show me the top 5 businesses with the most customer reviews.
    ``` sql
    WITH ranked_businesses AS (
        SELECT
            b.`business_id`,
            b.`name`,
            b.`latitude`,
            b.`longitude`,
            b.`city`,
            b.`state`,
            b.`review_count`,
            RANK() OVER (ORDER BY b.`review_count` DESC) AS rank
        FROM
            `demo_catalog`.`demo_yelp_academic`.`zz_silver_business` b
        WHERE
            b.`review_count` IS NOT NULL
    )
    SELECT
        `business_id`,
        `name`,
        `latitude`,
        `longitude`,
        `city`,
        `state`,
        `review_count`
    FROM
        ranked_businesses
    WHERE
        rank <= 5
    ```