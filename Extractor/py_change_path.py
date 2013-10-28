def change_path_name(infname, outfname):
	fd = open(infname)
	fd_w = open(outfname, 'w')
	for line in fd:
		line = line.strip().split('<<>>')		
		line[2] = line[2].replace('/media/9AFA0365FA033D4F/','/media/d/')
		fd_w.write('<<>>'.join(line) + '\n')
	fd_w.close()
	fd.close()

def read_url_path_to_id(fname):
	up2id = {}
	fd = open(fname)
	for line in fd:
		line = line.strip().split(' ')
		up2id[line[1]+line[2]] = line[1]
	fd.close()
	return up2id

def read_id_to_url_path(fname):
	id2up = {}
	fd = open(fname)
	for line in fd:
		line = line.strip().split('<<>>')
		if line[2].startswith('SEEUWO'):
			line[2] = '/media/d/' + line[2]
		id2up[line[0]] = line[1]+'<<>>'+line[2]
	fd.close()
	return id2up
		
def read_url_path_to_rank(fname):
	up2rank = {}
	fd = open(fname)
	for line in fd:
		line = line.strip().split('<<>>')
		up2rank[line[1]+'<<>>'+line[2]] = line[3]
	fd.close()
	return up2rank

def merge_with_rank(old_id_url_path_fname, new_rank_id_url_path_rank_fname, out_id_url_path_rank_list):
	id2up = read_id_to_url_path(old_id_url_path_fname)
	up2rank = read_url_path_to_rank(new_rank_id_url_path_rank_fname)
	ids = [int(id) for id in id2up.keys()]
	ids.sort()
	fd_w=  open(out_id_url_path_rank_list, 'w')
	fd = open('not.txt', 'w')
	for id in ids:
		id = str(id)
		up = id2up[id]
		if up in up2rank:
			rk = up2rank[up]
			fd_w.write(id + '<<>>' + up + '<<>>' + rk.strip() + '\n')
		else:
			fd.write(id + '<<>>' + up + '\n')			
	fd_w.close()
	fd.close()

def store_rank_to_db(fname, schema, table):
	import   MySQLdb  	
	con   =   MySQLdb.connect(host="192.168.0.2",  port=3306, user="root",  passwd="see",  db=schema)  
	cursor   =   con.cursor() 
	fd = open(fname) 
	for line in fd:
		line = line.strip().split(' ')		
		sql = 'insert into '+table+' values(' + line[0] + ',' + line[1] + ',' + line[2] + ')'
		cursor.execute(sql)			
	cursor.close()
	con.close()	
	fd.close()
	
if __name__ == '__main__':	
	old_id_url_path_fname = 'new_query_bing_id_url_path_list.txt'
	new_rank_id_url_path_rank_fname = 'new_query_bing_rank_modified_rank_id_url_path_rank_list.txt'
	out_id_url_path_rank_list = 'new_query_bing_rank_id_url_path_rank_list.txt'
	otuput_results = 'new_query_bing_rank_id_page_id_in_rank.txt'
	schem = 'query_search_engine'
	table = 'webdoc_id_result_rank'
	#change_path_name(out_id_url_path_rank_list, new_rank_id_url_path_rank_fname)
	"""
	merge_with_rank(old_id_url_path_fname, new_rank_id_url_path_rank_fname, out_id_url_path_rank_list)
	#remake rank
	fd_w = open(otuput_results, 'w')
	fd = open(out_id_url_path_rank_list)
	for line in fd:
		line = line.strip().split('<<>>')
		id = line[0]
		path = line[2]
		rank = line[3]
		a = path.rindex('/')
		b = path.rindex('.')
		pageid = path[a+1:b]
		fd_w.write(id + ' ' + pageid + ' ' + rank + '\n')		
	fd.close()
	fd_w.close()	
	"""
	store_rank_to_db(otuput_results, schem, table)
