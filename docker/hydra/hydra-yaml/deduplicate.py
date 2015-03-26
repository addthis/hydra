
import string
import tree


def mapIntersection(maps):
    if len(maps) == 0:
        return {}
    intersection = maps[0]
    for map in maps[1:]:
        remaining = {}
        for key in intersection:
            if key in map and tree.equals(intersection[key], map[key]):
                remaining[key] = intersection[key]
        intersection = remaining
    return tree.copy(intersection)

def yamlMapIntersection(maps):
    intersection = mapIntersection(maps)
    non_keys = filter(lambda k: k == "&" or k == "<<", intersection.keys())
    for non_key in non_keys:
        del intersection[non_key]
    return intersection

def deduplicateMaps(yamlTree, input_paths, output_path):
    subtrees = map(lambda x: tree.treeGet(yamlTree, x), input_paths)
    # make sure that subtrees is a non-empty list of (type) dictionaries
    if len(input_paths) == 0 or (not reduce(lambda x,y: x and y, map(lambda x: type(x) == dict, subtrees))):
        return tree.copy(yamlTree)
    intersection = yamlMapIntersection(subtrees)
    if len(intersection) == 0:
        return tree.copy(yamlTree)
    anchor = string.join(output_path, "_")
    for subtree in subtrees:
        for key in intersection:
            del subtree[key]
        merges = subtree.get("<<", [])
        merges.append("*" + anchor)
        subtree["<<"] = merges
    result = yamlTree
    for i in range(len(input_paths)):
        result = tree.treeSet(result, input_paths[i], subtrees[i])
    intersection["&"] = anchor
    return tree.treeSet(result, output_path, intersection)


