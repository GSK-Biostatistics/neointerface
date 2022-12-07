call apoc.cypher.run("
MATCH path=(left)-[r1]->(via)<-[r2]-(right)
WHERE left:`"+$left_label+"` and right:`"+$right_label+"` AND left <> right
WITH DISTINCT type(r1) as r1, labels(via) as lbls, type(r2) as r2, count(path) as cnt 
ORDER BY cnt DESC
UNWIND lbls as lbl
RETURN DISTINCT r1, lbl, r2
",
{}
) yield value
RETURN value['r1'] as r1, value['lbl'] as lbl, value['r2'] as r2