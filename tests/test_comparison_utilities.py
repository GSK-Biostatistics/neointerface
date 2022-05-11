from utils.neointerface_utils import *


def test_compare_unordered_lists():
    # Tests for the compare_unordered_lists function

    # POSITIVE tests
    assert compare_unordered_lists([1, 2, 3] , [1, 2, 3])
    assert compare_unordered_lists([1, 2, 3] , [3, 2, 1])
    assert compare_unordered_lists([] , [])
    assert compare_unordered_lists( ["x", (1, 2)]  ,  [(1, 2) , "x"] )

    # NEGATIVE tests
    assert not compare_unordered_lists( ["x", (1, 2)]  ,  ["x", (2, 1)] )
    assert not compare_unordered_lists( ["a", "a"]  ,  ["a"] )




def test_compare_recordsets():
    # Tests for the compare_recordsets function

    # POSITIVE tests

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ]
                             )  # Everything absolutely identical

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'patient_id': 123 , 'gender': 'M'} ]
                             )  # 2 fields reversed in last record of 2nd dataset

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'gender': 'M' , 'patient_id': 123} , {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ]
                             )  # Records reversed in 2nd dataset

    assert compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'patient_id': 123 , 'gender': 'M'} , {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'}  ]
                             )  # Records reversed in 2nd dataset, and fields reversed in one of them

    assert compare_recordsets(  [  {'gender': 'F', 'patient_id': 444, 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                [   {'patient_id': 123 , 'gender': 'M'} , {'patient_id': 444, 'condition_id': 'happy', 'gender': 'F'}  ]
                             )  # Additional order scrambling in the last test

    assert compare_recordsets([] , [])      # 2 empty datasets

    assert compare_recordsets([{'a': 1}] , [{'a': 1}])      # Minimalist data sets!

    assert compare_recordsets([{'a': 1}, {'a': 1}]  ,
                              [{'a': 1}, {'a': 1}])      # Each dataset has 2 identical records

    assert compare_recordsets([{'a': 1}, {'a': 1}, {'z': 'hello'}]  ,
                              [{'z': 'hello'}, {'a': 1}, {'a': 1}])     # Scrambled record order, with duplicates


    # NEGATIVE tests

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'}  ]
                                 )  # Missing record in 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'patient_id': 123} ]
                                 ) # Missing field in last record of 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123, 'extra_field': 'some junk'} ]
                                 ) # Extra field in 2nd dataset

    assert not compare_recordsets(  [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ],
                                    [  {'patient_id': 444, 'gender': 'F', 'condition_id': 'happy'} ,  {'gender': 'M' , 'patient_id': 123} ,  {'extra_record': 'what am I doing here?'} ]
                                 )  # Extra record in 2nd dataset

    assert not compare_recordsets( [] , [{'a': 1}] )      # one empty dataset and one non-empty

    assert not compare_recordsets([{'a': 1}]  ,
                                  [{'a': 1}, {'a': 1}])    # 1 record is NOT the same things as 2 identical ones

    assert not compare_recordsets([{'a': 1}, {'a': 1}, {'z': 'hello'}]  ,
                                  [{'a': 1}, {'a': 1}])     # datasets of different size
