def remove():
	data = {}
	fd = open('query_bing_id_url_path_list.txt')
	m = 0
	for line in fd:
		old_line = line
		line = line.strip().split('|')
		if len(line) > 3:
			m+= 1
		id = line[0]
		url = line[1]
		path = line[2]
		if path.startswith('/media/d/SEEUWO_Training_Data/'):
			path = path[len('/media/d/SEEUWO_Training_Data/'):]
		elif path.startswith('/home/xiao/wiki_subject_hierarchy/SEEUWO_Training_Data_top_two/') != -1:
			path = path[len('/home/xiao/wiki_subject_hierarchy/SEEUWO_Training_Data_top_two/'):]
		elif path.startswith('SEEUWO_Training_Data/') != -1:
			path = path[len('SEEUWO_Training_Data/'):]	
		if url + path not in data:
			data[url + path] = id
		else:
			print  url + '    '+ path + '    ' + old_line
	fd.close()
	fd_w = open('unique_query_bing_actual_id_list.txt', 'w')
	for d in data.values():	
		fd_w.write(d + '\n')
	fd_w.close()

ids = []
fd = open('unique_query_bing_actual_id_list.txt')
for line in fd:
	ids.append(int(line.strip()))
fd.close()
ids.sort()
fd_w = open('sorted_unique_query_bing_actual_id_list.txt', 'w')
for id in ids:
	fd_w.write(str(id) + '\n')
fd_w.close()
