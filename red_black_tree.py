# Reference: https://brilliant.org/wiki/red-black-tree/
class Node:
    def __init__(self, key):
        self.key = key
        self.red = True
        self.left = None
        self.right = None
        self.parent = None

class RedBlackTree:
    def __init__(self):
        self.root = None

    def search(self, key):
        currentNode = self.root
        while currentNode != None and key != currentNode.key:
            if key < currentNode.key:
                currentNode = currentNode.left
            else:
                currentNode = currentNode.right
        return currentNode

    def insert(self, key):
        node = Node(key)
        #Base Case - Nothing in the tree
        if self.root == None:
            node.red = False
            self.root = node
            return
        #Search to find the node's correct place
        currentNode = self.root
        while currentNode != None:
            potentialParent = currentNode
            if node.key < currentNode.key:
                currentNode = currentNode.left
            else:
                currentNode = currentNode.right
        #Assign parents and siblings to the new node
        node.parent = potentialParent
        if node.key < node.parent.key:
            node.parent.left = node
        else:
            node.parent.right = node
        self.fixTree(node)

    def delete():
        currentNode = self.root
        while currentNode != None:
            



    def fixTree(self, node):
        while node.parent.red == True and node != self.root:
            if node.parent == node.parent.parent.left:
                uncle = node.parent.parent.right
                if uncle.red:
                    #This is Case 1
                    node.parent.red = False
                    uncle.red = False
                    node.parent.parent.red = True
                    node = node.parent.parent
                else:
                    if node == node.parent.right:
                        #This is Case 2
                        node = node.parent
                        self.left_rotate(node)
                    #This is Case 3
                    node.parent.red = False
                    node.parent.parent.red = True
                    self.right_rotate(node.parent.parent)
            else:
                uncle = node.parent.parent.left
                if uncle.red:
                    #Case 1
                    node.parent.red = False
                    uncle.red = False
                    node.parent.parent.red = True
                    node = node.parent.parent
                else:
                    if node == node.parent.left:
                        #Case 2
                        node = node.parent
                        self.right_rotate(node)
                    #Case 3
                    node.parent.red = False
                    node.parent.parent.red = True
                    self.left_rotate(node.parent.parent)
        self.root.red = False

    def left_rotate(self, node):
        sibling = node.right
        node.right = sibling.left
        #Turn sibling's left subtree into node's right subtree
        if sibling.left != None:
            sibling.left.parent = node
        sibling.parent = node.parent
        if node.parent == None:
            self.root = sibling
        else:
            if node == node.parent.left:
                node.parent.left = sibling
            else:
                node.parent.right = sibling
        sibling.left = node
        node.parent = sibling

    def right_rotate(self, node):
        sibling = node.left
        node.left = sibling.right
        if sibling.right != None:
            sibling.right.parent = node
        sibling.parent = node.parent
        if node.parent == None:
            self.root = sibling
        else:
            if node == node.parent.right:
                node.parent.right = sibling
            else:
                node.parent.left = sibling
        sibling.right = node
        node.parent = sibling
