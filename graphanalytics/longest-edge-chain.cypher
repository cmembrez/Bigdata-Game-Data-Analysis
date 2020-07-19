//Longest Edge Chain
MATCH path=()-[:ResponseTo*]->()
WHERE length(path)=9
MATCH uCount=(u:User)-[r:CreateChat]->(i:ChatItem)
WHERE (i IN NODES(path))
RETURN uCount