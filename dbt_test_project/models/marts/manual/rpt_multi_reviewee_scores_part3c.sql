WITH
manual_reviews AS (
    SELECT
        reviewee_id,
        score
    FROM {{ ref('stg_klaus_manual_reviews') }}
    WHERE score IS NOT NULL
),

reviewees_with_multiple_reviews AS (
    SELECT reviewee_id
    FROM manual_reviews
    GROUP BY 1
    HAVING count(*) > 1
),

final AS (
    SELECT
        mr.reviewee_id,
        avg(mr.score) AS avg_score
    FROM manual_reviews AS mr
    INNER JOIN reviewees_with_multiple_reviews rwmr ON mr.reviewee_id = rwmr.reviewee_id
    GROUP BY 1
)

SELECT *, current_timestamp() AS run_time FROM final