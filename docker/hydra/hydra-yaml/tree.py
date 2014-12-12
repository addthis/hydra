

def copy(a):
    if type(a) == dict:
        out = {}
        for key in a:
            out[copy(key)] = copy(a[key])
        return out
    elif type(a) == list:
        out = []
        for b in a:
            out.append(copy(b))
        return out
    return a

def equals(a, b):
    if type(a) == dict and type(b) == dict:
        for key in a:
            if key not in b or not equals(a[key], b[key]):
                return False
        return True
    elif type(a) == list and type(b) == dict:
        if len(a) != len(b):
            return False
        for i in range(len(a)):
            if not equals(a[i], b[i]):
                return False
        return True
    return a == b

def treeGet(tree, path):
    if tree == None or len(path) == 0:
        return copy(tree)
    return treeGet(tree.get(path[0]), path[1:])

def treeSet(tree, path, value):
    if len(path) == 0:
        return copy(value)
    result = copy(tree)
    result[path[0]] = treeSet(tree.get(path[0], {}), path[1:], value)
    return result



