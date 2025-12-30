SELECT symbol, SUM(views) AS total_views 
FROM sentiment_db.public.sentiment_data 
WHERE 
    POSITION('amazon' IN LOWER(symbol)) > 0 OR
    POSITION('apple' IN LOWER(symbol)) > 0 OR
    POSITION('facebook' IN LOWER(symbol)) > 0 OR
    POSITION('google' IN LOWER(symbol)) > 0 OR
    POSITION('microsoft' IN LOWER(symbol)) > 0
GROUP BY symbol 
ORDER BY total_views DESC;