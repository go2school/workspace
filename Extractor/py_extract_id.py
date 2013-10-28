def extract_id(inf, outf):
	fd=  open(inf)
	fd_w = open(outf, 'w')
	for line in fd:
		a = line.index(' ')
		fd_w.write(line[:a] + '\n')
	fd_w.close()
	fd.close()

def read_id(fname):
	ids = set()
	fd = open(fname)
	for line in fd:
		ids.add(line.strip())
	fd.close()
	return ids

def write_ids(ids, fname):
	vids = [int(i) for i in ids]
	vids.sort()
	fd = open(fname, 'w')
	for v in vids:
		fd.write(str(v) + '\n')
	fd.close()
	
#fd=  open('new_query_bing_rank_id_page_id_in_rank.txt')
#fd_w = open('new_ids.txt', 'w')
#extract_id('/home/mpi/uwo_query_bing_workingspace/new_query_bing_labels.txt', 'training_ids.txt')

#ids_1 = read_id('new_ids.txt')
#ids_2 = read_id('training_ids.txt')
#write_ids(ids_2 -  (ids_2 & ids_1), 'diff.txt')
ids =set()
fd = open('new_new_query_bing_labels.txt')
for line in fd:
	a = line.index(' ')
	ids.add(line[:a])
fd.close()
fd = open('new_query_bing_rank_id_page_id_in_rank.txt')
fd_w = open('new_new_query_bing_rank_id_page_id_in_rank.txt', 'w')
for line in fd:
	a = line.index(' ')
	if line[:a] in ids:
		fd_w.write(line)
fd_w.close()
