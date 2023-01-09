import hashlib, json, sys
import copy
import random
random.seed(0)

def hashFunc(message = ""):
    if type(message) != str:
        # sort key in order to guarantee repeatability
        message = json.dumps(message,sort_keys = True)
    return hashlib.sha256(str(message).encode('utf-8')).hexdigest()

# Used to simulate some transactions
def makeTransaction(maxValue = 10):
    # create valid transactions in range of (1,maxValue)
    sign = int(random.getrandbits(1)) * 2 - 1
    amount = random.randint(1,maxValue)
    alice_pay = sign * amount
    bob_pay = -1 * alice_pay
    return {'Alice':alice_pay, 'Bob':bob_pay}

# [{Alice:...,Bob:...},{Alice:...,Bob:...}...]
transactionBuffer = [makeTransaction() for i in range(30)]

def updateState(trans, state):
    # state is the account balance
    state = state.copy()
    for key in trans:
        # different from C++, init value != 0
        if key in state.keys():
            state[key] += trans[key]
        else:
            state[key] = trans[key]
    return state

def checkValid(trans,state):
    if sum(trans.values()) != 0:
        return False
    for key in trans.keys():
        if key in state.keys():
            account_balance = state[key]
        else:
            account_balance = 0
        # Check if current amount < amount that user can pay
        if(account_balance + trans[key]) < 0:
            return False
    return True

state = {'Alice':500, 'Bob':500}
# init one block
genesis_block_trans = [state]
genesis_block_content = {'blockNumber':0, 'parentHash':None, 'transactionCount':1, 'transactions':genesis_block_trans}
genesis_hash = hashFunc(genesis_block_content)
genesis_block = {'hash':genesis_hash, 'content': genesis_block_content}
genesis_block_str = json.dumps(genesis_block,sort_keys=True)
# init the chain, add the init block to chain
chain = [genesis_block]

def makeBlock(transactions, chain):
    parent_block = chain[-1]
    parent_hash = parent_block['hash']
    parent_content = parent_block['content']
    block_number = parent_content['blockNumber'] + 1
    trans_count = len(transactions)
    new_block_content = {'blockNumber':block_number, 'parentHash':parent_hash, 'transactionCount':trans_count, 'transactions':transactions}
    new_block_hash = hashFunc(new_block_content)
    new_block = {'hash':new_block_hash,'content':new_block_content}
    return new_block

# 5 transactions per block
block_size_limit = 5

while len(transactionBuffer) > 0:
    buffer_state_size = len(transactionBuffer)
    trans_list = []
    while (len(transactionBuffer)>0) and (len(trans_list) < block_size_limit):
        new_trans = transactionBuffer.pop()
        valid = checkValid(new_trans,state)
        if valid == True:
            trans_list.append(new_trans)
            state = updateState(new_trans,state)
        else:
            sys.stdout.flush()
            continue
    my_block = makeBlock(trans_list,chain)
    chain.append(my_block)

def checkBlockHash(block):
    expected_hash = hashFunc(block['content'])
    if block['hash'] != expected_hash:
        raise Exception('Hash does not match contents of block %s' % block['contents']['blockNumber'])
    return

def checkBlockValid(block, parent, state):
    parent_number = parent['content']['blockNumber']
    parent_hash = parent['hash']
    block_number = block['content']['blockNumber']
    for trans in block['content']['transactions']:
        if checkValid(trans,state) == True:
            state = updateState(trans,state)
        else:
            raise Exception('Invalid transaction in block %s: %s' % (block_number, trans))

    checkBlockHash(block)

    if block_number != (parent_number + 1):
        raise Exception('Block number does not correct %s' % block_number)

    if block['content']['parentHash'] != parent_hash:
        raise Exception('Parent hash not accurate at block %s' % block_number)
    return state

def checkChain(chain):
    if type(chain) == str:
        try:
            chain = json.loads(chain)
            assert(type(chain)==list)
        except:
            return False
    state = {}
    for trans in chain[0]['content']['transactions']:
        state = updateState(trans,state)
    checkBlockHash(chain[0])
    parent = chain[0]
    for block in chain[1:]:
        state = checkBlockValid(block,parent,state)
        parent = block
    return state

node_blockchain = copy.copy(chain)
node_transactions = [makeTransaction() for i in range(5)]
new_block = makeBlock(node_transactions,node_blockchain)

print("Blockchain on Node A is currently %s blocks long" % len(chain))
try:
    print("New Block Received; checking validity...")
    state = checkBlockValid(new_block,chain[-1],state)
    chain.append(new_block)
except:
    print("Invalid block; Ignore it and waiting for the next block.")

print("Blockchain on Node A is now %s blocks long" % len(chain))







