import pytest
import neointerface
import pandas as pd


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface()
    yield neo_obj


def test_extract_entities(db):
    # Completely clear the database
    db.clean_slate()
    # Create minimalist test data.  See image "extract_class_entities/BEFORE _extract_class_entities_part_2.png"
    q1 = """ 
        CREATE 
           (:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"})          
        """
    db.query(q1)

    db.extract_entities(mode='merge',
                        cypher='''
                                   MATCH (f:`Source Data Table`{_domain_:$domain})-[:HAS_DATA]->(node:`Source Data Row`)
                                   RETURN id(node)
                                   ''',
                        cypher_dict={'domain': 'Automotive'},
                        target_label='car',
                        property_mapping={'car_color': 'color'},
                        relationship='FROM_DATA',
                        direction='<'
                        )

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/AFTER _extract_class_entities_part_2.png"
    cypher = "MATCH (n:car {color:'white'})-[rel:FROM_DATA]->(m:`Source Data Row` {car_color:'white'}) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_extract_entities2(db):
    db.clean_slate()
    df = pd.DataFrame({'id': [1, 2, 3, 4, 5], 'color': ['red', 'red', 'red', 'blue', 'blue']})
    db.load_df(df, label='Thing')
    db.extract_entities(
        label='Thing',
        target_label='Color',
        relationship='OF',
        property_mapping=['color'])
    things = db.get_nodes("Thing")
    colors = db.get_nodes("Color")
    assert things == df.to_dict(orient='records')
    assert colors == [{'color': 'red'}, {'color': 'blue'}]


def test_extract_empty(db):
    # Completely clear the database
    db.clean_slate()
    # Create minimalist test data.  See image "extract_class_entities/BEFORE _extract_class_entities_part_2.png"
    q1 = """ 
        CREATE 
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "black"})
        """
    db.query(q1)

    db.debug = True
    db.extract_entities(mode='create',
                        # empty nodes extraction is allowed in 'create' mode, in 'merge' mode apoc.merge.node fails
                        label='Source Data Row',
                        target_label='car',
                        property_mapping={},
                        relationship='FROM_DATA',
                        direction='<'
                        )

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/AFTER _extract_class_entities_part_2.png"
    cypher = "MATCH (car)-[rel:FROM_DATA]->(m:`Source Data Row`) RETURN count(*) as cnt, sum(size(keys(car))) as nprop"
    result = db.query(cypher)
    assert result[0]['cnt'] == 2
    assert result[0]['nprop'] == 0


def test_extract_multilabel(db):
    # Completely clear the database
    db.clean_slate()
    # Create minimalist test data.  See image "extract_class_entities/BEFORE _extract_class_entities_part_2.png"
    q1 = """ 
        CREATE 
           (sdt:`Source Data Table` {_domain_: "Automotive"})-[:HAS_DATA]->(:`Source Data Row` {car_color: "white"}),
           (sdt)-[:HAS_DATA]->(:`Source Data Row` {car_color: "black"})
        """
    db.query(q1)

    db.debug = True
    db.extract_entities(mode='create',
                        label='Source Data Row',
                        target_label=['Car', 'Bw_car'],
                        property_mapping={},
                        relationship='FROM_DATA',
                        direction='<'
                        )

    # Verify that the expected node and relationship got created.
    # See the image "extract_class_entities/AFTER _extract_class_entities_part_2.png"
    cypher = "MATCH (car:Car:Bw_car) RETURN *"
    result = db.query(cypher)
    assert len(result) == 2
