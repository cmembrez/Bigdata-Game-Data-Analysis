// Cluster Coefficients
MATCH (u1:User)-[r1:InteractsWith]->(u2:User) 
WHERE  u1.id IN [394,2067,1087,209,554,1627,516,999,668,461]
AND (u1.id <> u2.id)
WITH COLLECT(u2.id) AS neighbours, COUNT(DISTINCT r1) AS k, u1 
MATCH (u3:User)-[r2:InteractsWith]->(u4:User)
WHERE (u3.id IN neighbours) AND (u4.id IN neighbours) AND (u3.id <> u4.id)
WITH u1, u3, u4, k,
CASE WHEN COUNT(r2) > 0 THEN 1
ELSE 0
END AS result
RETURN u1.id, SUM(result)*1.0/(k*(k-1)) AS coef
ORDER BY coef DESC LIMIT 10