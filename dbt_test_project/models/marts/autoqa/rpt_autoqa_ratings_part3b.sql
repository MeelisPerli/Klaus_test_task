WITH
final AS (
    SELECT
        external_ticket_id,
        avg(score) AS avg_score
    FROM {{ ref('stg_klaus_autoqa_ratings') }}
    WHERE score is not null
    GROUP BY 1
)

SELECT *, current_timestamp() AS run_time FROM final
