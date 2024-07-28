WITH

final AS (
    SELECT
        review_id,
        sum(rating * weight)/nullif(sum(rating_max*weight), 0)*100 AS rating_score_percentage
    FROM {{ ref('stg_klaus_manual_rating') }}
    WHERE rating is not null and rating != 42
    GROUP BY 1
    HAVING rating_score_percentage is not null
)

SELECT *, current_timestamp() AS run_time FROM final
