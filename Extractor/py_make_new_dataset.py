def make_dataset():
	ids = set()
	fd = open('diff.txt')
	for line in fd:
		ids.add(line.strip())
	fd.close()
	fd = open('/home/mpi/uwo_query_bing_workingspace/new_query_bing_labels.txt')
	fd_w = open('new_new_query_bing_labels.txt', 'w')
	for line in fd:
		a = line.index(' ')
		id = line[:a]
		if id not in ids:
			fd_w.write(line)
	fd.close()
	fd_w.close()

def check_empty(infname, catids):
	labels = {}
	fd=  open(infname)
	for line in fd:
		line = line.strip().split(' ')
		for l in line[1:]:
			if l in labels:
				labels[l] += 1
			else:
				labels[l] = 1
	fd.close()
	#read id
	ids = set()
	fd = open(catids)
	for line in fd:
		ids.add(line.strip())
	fd.close()
	vs = {}
	for id in ids:
		if id not in labels:
			print id + ' not in '
		else:
			vs[id] = labels[id]
	return labels, vs

def check_correctness(infeature, inlabel, inrankid):
	f_ids = []
	l_ids = []
	fd = open(infeature)
	for line in fd:
		a = line.index(' ')
		f_ids.append(line[:a])
	fd.close()		
	fd = open(inlabel)
	for line in fd:
		a = line.index(' ')
		l_ids.append(line[:a])
	fd.close()
	f_ranks = []
	fd = open(inrankid)
	for line in fd:
		a = line.index(' ')
		f_ranks.append(line[:a])
	fd.close()
	if len(f_ids) != len(l_ids) and len(f_ids) != len(f_ranks):
		print 'does not equal in length'
		return
	else:
		for i in range(len(f_ids)):
			if f_ids[i] != l_ids[i]:
				print 'does not equal '	
		for i in range(len(f_ids)):
			if f_ids[i] != f_ranks[i]:
				print 'does not equal '	
							
infeature = '/home/mpi/uwo_query_bing_workingspace/new_query_bing_features.txt'
outfeature = 'new_new_query_bing_features.txt'							
outlabel = 'new_new_query_bing_labels.txt'			
out_ranks = 'new_new_query_bing_rank_id_page_id_in_rank.txt'	
#ls = check_empty(outlabel, 'used_tree_nodes.txt')
"""
ids = set()
fd = open('diff.txt')
for line in fd:
	ids.add(line.strip())
fd.close()

fd = open(infeature)
fdw = open(outfeature, 'w')
for line in fd:
	a = line.index(' ')
	if line[:a] not in ids:
		fdw.write(line)
fdw.close()
fd.close()
"""
check_correctness(outfeature, outlabel, out_ranks)
