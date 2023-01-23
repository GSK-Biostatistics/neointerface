call apoc.cypher.run("
MATCH path=(left)-[r1]-(via)-[r2]-(right)
WHERE left:`"+$left_label+"` and right:`"+$right_label+"` AND left <> right
WITH DISTINCT type(r1) as r1, labels(via) as lbls, type(r2) as r2, count(path) as cnt,
    CASE WHEN endNode(r1) = via THEN '>' ELSE '<' END as r1_dir, 
    CASE WHEN endNode(r2) = via THEN '<' ELSE '>' END as r2_dir
ORDER BY cnt DESC
UNWIND lbls as lbl
RETURN DISTINCT r1, lbl, r2, r1_dir, r2_dir
",
{}
) yield value
RETURN value['r1'] as r1, value['lbl'] as lbl, value['r2'] as r2, value['r1_dir'] as r1_dir, value['r2_dir'] as r2_dir