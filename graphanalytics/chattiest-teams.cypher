//Chattiest Teams
MATCH (c:ChatItem)-[:PartOf]->(:TeamChatSession)-[:OwnedBy]->(t:Team)
RETURN t.id, count(c) AS chats 
ORDER BY chats DESC LIMIT 10