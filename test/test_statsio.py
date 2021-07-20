import pytest
import boto3
from moto import mock_s3

from tacostats.statsio import s3, local
from test.utils import create_bucket, create_obj

def ugh():
    with mock_s3():                                                        
        create_bucket() 
        create_obj(key='prefix/fakeobj.json')
        s3 = boto3.client('s3')
        print(s3.list_objects_v2(Bucket="tacostats-data", Prefix="prefix/fakeobj")     )

def test_s3_get_age_one_obj():
    # one obj
    with mock_s3():
        create_bucket()
        create_obj(key='prefix/fakeobj.json')
        age = s3.get_age("prefix", "fakeobj")
        assert age
        assert isinstance(age, int)
        assert age >= 0

def test_s3_get_age_two_obj():
    # 2 objects, only care if it freaks out.
    with mock_s3():
        create_bucket()
        create_obj(key='prefix/fakeobj.json')
        create_obj(key='prefix/fakeobj2.json')
        age = s3.get_age("prefix", "fakeobj")
        assert age
        assert isinstance(age, int)
        assert age >= 0

def test_s3_get_age_no_obj():
    # obj does not exist
    with mock_s3():
        create_bucket()
        with pytest.raises(KeyError):
            s3.get_age("prefix", "fakeobj")
