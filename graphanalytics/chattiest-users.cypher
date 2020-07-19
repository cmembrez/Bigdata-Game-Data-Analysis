// Chattiest Users
MATCH (n:User)-[r:CreateChat]->()
RETURN n.id ,count(r)
ORDER BY count(r) DESC LIMIT 10