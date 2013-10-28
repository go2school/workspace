fd = open('query_bing_empty_text_id_url_path_list.txt')
fd_w_1 = open('see_query_bing_empty_text_id_url_path_list.txt', 'w')
fd_w_2 = open('local_query_bing_empty_text_id_url_path_list.txt', 'w')
for line in fd:
	tmp_l = line
	line = line.strip()
	b = line.rindex('|')
	if line[b+1:].startswith('SEEUWO_Training_Data'):		
		fd_w_1.write(tmp_l)
	elif  line[b+1:].startswith('/home'):
		fd_w_2.write(tmp_l)
	else:
		print 'bad'
fd_w_1.close()
fd_w_2.close()
fd.close()
