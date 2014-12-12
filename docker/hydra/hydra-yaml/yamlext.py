
import string
import tree


def readFile(path):
    f = open(path, 'r')
    content = f.read()
    f.close()
    return content

def indent(text, spacing="    "):
    return spacing + string.join(string.split(text, "\n"), "\n" + spacing)

def dumpYaml(yamlTree):
    if type(yamlTree) == dict:
        copy_tree = tree.copy(yamlTree)
        output = ""
        if "&" in copy_tree:
            output = output + "&" + copy_tree["&"] + " "
            del copy_tree["&"]
        if "<<" in copy_tree:
            for merge in reversed(copy_tree["<<"]):
                output = output + "\n<<: " + merge 
            del copy_tree["<<"]
        def normalizeKey(key):
            if key == "default":
                return ("", key)
            return (key.upper(), key)
        sorted_keys = map(normalizeKey, copy_tree.keys())
        sorted_keys.sort()
        for (upper,key) in sorted_keys:
            subtree = dumpYaml(yamlTree[key])
            if type(yamlTree[key]) == dict:
                subtree = indent(subtree)
            output = output + "\n" + key + ": " + subtree
        return output
    elif type(yamlTree) == str and len(string.split(yamlTree)) > 0 and string.count(yamlTree, "\n") > 0 :
        words = ""
        for word in string.split(yamlTree):
            words = words + word + "\n"
        return ">\n" + indent(string.strip(words))
    elif type(yamlTree) == str:
        return "\"" + yamlTree + "\""
    elif type(yamlTree) == type(None):
        return ""
    else:
        return str(yamlTree)

def formatKeyValue(key, value):
    value = str(value).strip()
    # make booleans lower case
    if value.lower() in ["true", "false"]:
      value = value.lower()
    # check XX first so X does not steal it
    if key.startswith("XX"):
        key = key[2:]
        if value in ["true", "+"]:
            return "-XX:+%s" % key
        elif value in ["false", "-"]:
            return "-XX:-%s" % key
        else:
            return "-XX:%s=%s" % (key, value)
    if key.startswith("X"):
        return "-%s%s" % (key, value)
    if key == "RawOptions":
        return value
    return "-D%s=%s" % (key, value)

def dumpJavaDefines(atree):
    formatedParams = [formatKeyValue(key, value) for (key, value) in atree.items()]
    return " " + " ".join(formatedParams)
