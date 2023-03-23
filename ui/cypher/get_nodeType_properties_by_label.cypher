call db.schema.nodeTypeProperties() yield nodeLabels, propertyName
WHERE $label in nodeLabels
WITH distinct propertyName
RETURN propertyName order by propertyName