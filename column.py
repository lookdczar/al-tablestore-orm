

class Column():
    def __init__(self, json_obj = False) -> None:
        """_

        Args:
            json_obj (bool, optional): 是否为需要做json序列化的对象. Defaults to False.
        """
        self.name = ''
        self.json_obj = json_obj


