import sys, os
file_path = os.path.split(os.path.realpath(__file__))[0]
parent = os.path.dirname(file_path)
sys.path.append(parent)
os.chdir(parent)
__package__ = os.path.basename(file_path)


from .column import Column
from .primary_key import PrimaryKey
from .base import BaseMode
from .filter import Filter

from .client import init_tb_client, client
from .util import hash_id

init_tb_client(end_point='', access_key_id='', access_key_secret='', instance_name=''
               )

class Test(BaseMode):
    __table__ = 'comic_img'
    
    id = PrimaryKey(index=0)
    aid = PrimaryKey(index=1, sce_index_tb_name='comic_img_index_aid')
    image_id = PrimaryKey(index=2)

    source_url = Column()
    aly_parent_file_id = Column()
    aly_file_name = Column()
    aly_file_id = Column()
    aly_url = Column()
    local_path = Column()
    
    

if __name__ == "__main__" :

    # test = Test.find_by_pk([hash_id('445845'), 445845, '00011'])
    models = Test.query_by_pk(pk_filters=[
        Filter.is_(Test.aid, 445845)
    ])
    print(models)