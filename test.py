
class Node:
    def __init__ (self,id: int, item_id: int, tag_id: int):
        self.id = id
        self.item_id = item_id
        self.tag_id = tag_id
        self.max = 1
        self.nodesout = None
        self.relations = None
        
    def add_nodesout(self, nodesout):
        self.nodesout = nodesout
    def add_relations(self, nodes):
        self.relations = nodes
    def add_max(self, max:int):
        self.max = max



class Chain:
    def __init__ (self, node: Node, father: "Chain"):
        self.node = node       
        self.father = father
        if father is not None:
            self.size = father.size + 1       
            self.nodesout = node.nodesout | father.nodesout


def test():
    chains = {}
    for node in nodes.values():
        chains[node.id] = [Chain(node, None)]

    df_relations["active"] = True
    chains_found = []
    max = 0
    counter = 0
    while not chains_found:
        min_date = df_relations.loc[df_relations["active"], "finish2"].min()

        next = df_relations[df_relations["active"] & (df_relations["finish2"] == min_date)]

        df_relations.loc[next.index, "active"] = False

        next_node = next["node_id1"].values
        activated_nodes = set()
       
        for _, row in next.iterrows():
            chains_to_remove = []
            for chain in chains[row["node_id1"]]:
                if chain.try_remove_next(nodes[row["node_id2"]]):
                    test2 = nodes[row["node_id2"]]
                    new_chain = Chain(nodes[row["node_id2"]], chain)
                    chain.remove_next(nodes[row["node_id2"]])
                    if not chain.next.any():
                        chains_to_remove.append(chain)
                        chain.delete_attribute()
                    if new_chain.size > max:
                        max = new_chain.size
                    if new_chain.size >= 15:
                        chains_found.append(new_chain)
                    elif new_chain.next.any():
                        chains[row["node_id2"]].append(new_chain)
                        activated_nodes.add(row["node_id2"])
                        counter +=1
                    else:
                        del new_chain

                
            for chain in chains_to_remove:
                chains[row["node_id1"]].remove(chain)
                counter -=1

        df_relations.loc[df_relations["node_id1"].isin(activated_nodes), "active"] = True
        print(str(counter) + " chains active and max " + str(max))

    for chain in chains_found:
        stop = True
        id = []
        tag_id = []
        node_id = []
        a = chain
        while a != None:
            id.append(a.node.item.id)
            tag_id.append(a.node.tag.id)
            node_id.append(a.node.id)
            a = a.father

        print("Found chain of size " + str(node_id))
        print("Found chain of size " + str(chain.size) + " with id " + str(id) + " and tag_id " + str(tag_id))


    snapshot = tracemalloc.take_snapshot()
    stats = snapshot.statistics("lineno")
    for stat in stats[:10]:
        print(stat)

            




            