class ActionError(Exception):
    def __init__(self, message, description=None):
        Exception.__init__(self, message)
        self.descripton = description

