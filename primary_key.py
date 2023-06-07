

class PrimaryKey():
    def __init__(self, index, autoincrement=False, sce_index_tb_name = '') -> None:
        self.index = index
        self.name = ''
        self.autoincrement = autoincrement
        self.sce_index_tb_name = sce_index_tb_name