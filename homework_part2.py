import random

#%% 1. Дан массив целых чисел nums и целое число target. Написать функцию, возвращающую индексы элементов, дающих в сумме число target?

def func(nums, target, toggle=True):
    
    ''' nums - массив целых чисел (list or tuple)
        target - integer
        toggle - если True, то складывать число с самим собой нельзя
    '''
    if not isinstance(lst, (list, tuple)):
        raise TypeError( 'Должен быть массив целых чисел (list or tuple)') 
        
    idx = []
    for i in range(len(nums)):
        for j in range(len(nums)):
            
            if i == j and toggle:
                continue 
            
            if nums[i]+nums[j] == target:
                
                if (j, i) in idx:
                    continue
                else:
                    idx.append((i, j))
    if idx:        
        return idx
    else:
        txt = 'Среди данного массива нет чисел отвечающих условиям задачи'
        return txt

# генерируем массив из случайных чисел
# random.seed(33)
rng = range(random.randint(1, 10))
lst = [random.randint(1, 10) for i in rng]
print('Массив: ', lst)

# генерируем случайное число - target
# random.seed(33)
target = random.randint(1, 15)
print('target:', target)

res = func(lst, target, False)
print('Искомые пары индексов:', res)

#%%
 
# 2. Предполагается, что в каждом массиве имеется не больше одной пары дающих в сумме заданное число target. Нельзя использовать один и тот же элемент дважды?

# Можно использовать такой элемент дважды в силу коммутативности операции сложения (коммутативное кольцо).

#%%
import numpy as np 
import math
import time 
# import multiprocessing
import concurrent.futures
#%%
nwords = 100
np.random.seed(32)
idx = np.random.randint(nwords, size=nwords*10)
words = list(map(lambda i: f'word{i}', idx))
words = np.array(words)[idx].tolist()
# print(words)

#%% -------------MAP----------------------------


def split(words, nchunks):
    '''split data into chunks'''
    
    step = math.ceil(len(words)/nchunks)
    
    for i in range(0, len(words), step):
        yield words[i:i+step]

def worker_counter(chunk, numb):
    '''
        worker that count words in a collection
        chunk: a collection
        numb: a worker number
    '''
    dct = {}  # dictionary to store word counters
    
    print('Starting process by worker {}'.format(numb))
    
    while chunk:
        
        
        word = chunk.pop()        
        
        if word in dct.keys():
            dct[word] += 1 
        else:
            dct [word] = 1 
    
    # time.sleep(1)            
    print(f'Process is finished by worker {numb}')
    
    return dct

# ----------MAP-------------------------
def map1(words, nchunks=2):

    processes = []  # list to store current workers
    results = [] # list to store all results of workers
    splitted_data = split(words, nchunks) # generator function

    # start time counting
    start_time = time.perf_counter()
    
    # multithread
    with concurrent.futures.ProcessPoolExecutor() as executor:
        
        for numb, chunk in enumerate(splitted_data, start=1):
            
            proc = executor.submit(worker_counter, chunk, numb)
            processes.append(proc)
   
    for f in concurrent.futures.as_completed(processes):
        results.append(f.result())
    
    end_time = time.perf_counter()        
    print(f'Finished  in {round(end_time - start_time, 4)} second(s)')            

    return results
time.sleep(1)
print('*****mapping: splitting and processing***')
results = map1(words, 2)

# ----------------- REDUCE -------------------------
def reduce(results):
    
    dct = {}  # final result
    for worker in results:        
        for key in worker.keys():
            
            if key in dct.keys():
                dct[key] += worker[key]
            else:
                dct[key] = worker[key]
    
    return dct

print('****reducing: combining results of mapping******')
time.sleep(1)
res = reduce(results)
res = dict(sorted(res.items(), key=lambda x: x[1], reverse=True))
print(res)            









