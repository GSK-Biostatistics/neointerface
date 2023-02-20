import pytest
import neointerface


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface()
    yield neo_obj


def test_link_entities_default(db):
    # Completely clear the database
    db.clean_slate()
    left = db.create_node_by_label_and_dict("apple")
    right = db.create_node_by_label_and_dict("fruit")
    sdr = db.create_node_by_label_and_dict("Source Data Row")
    db.link_nodes_by_ids(left, sdr, "FROM_DATA")
    db.link_nodes_by_ids(right, sdr, "FROM_DATA")

    db.link_entities(left_class='apple', right_class='fruit',
                     cond_via_node="Source Data Row",
                     cond_left_rel="FROM_DATA>",
                     cond_right_rel="<FROM_DATA",)
    cypher = "MATCH (l:apple)-[rel:HAS_FRUIT]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_link_entities(db):
    # Completely clear the database
    db.clean_slate()
    left = db.create_node_by_label_and_dict("apple")
    right = db.create_node_by_label_and_dict("fruit")
    sdr = db.create_node_by_label_and_dict("My Data")
    db.link_nodes_by_ids(left, sdr, "FROM")
    db.link_nodes_by_ids(right, sdr, "FROM2")

    db.link_entities(
        left_class='apple',
        right_class='fruit',
        relationship="IS_A",
        cond_via_node="My Data",
        cond_left_rel="FROM>",
        cond_right_rel="<FROM2"
    )
    cypher = "MATCH (l:apple)-[rel:IS_A]->(r:fruit) RETURN rel"
    result = db.query(cypher)
    assert len(result) == 1


def test_link_entities_cypher(db):
    # Completely clear the database
    db.clean_slate()
    left = db.create_node_by_label_and_dict("apple", {"type": "Pink lady"})
    left = db.create_node_by_label_and_dict("apple", {"type": "Granny Smith"})
    right = db.create_node_by_label_and_dict("fruit")

    # 1
    db.link_entities(
        left_class='apple',
        right_class='fruit',
        relationship="IS_A",
        cond_cypher="MATCH (left:apple{type:$apple_type}), (right:fruit) RETURN left, right",
        cond_cypher_dict={'apple_type': 'Pink lady'},
        cond_left_rel="IS_A>",
        cond_right_rel="<IS_A",
    )
    cypher = "MATCH path1=(a1:apple{type:'Pink lady'})-[rel:IS_A]->(r:fruit), (a2:apple{type:'Granny Smith'}) RETURN *"
    result = db.query(cypher)
    assert len(result) == 1
