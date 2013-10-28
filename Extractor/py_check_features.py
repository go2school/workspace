def get_length():
	lends = []
	fd = open('query_bing_id_svm.txt')
	for line in fd:
		line = line.strip().split(' ')
		lends.append((line[0], len(line[1:])))
	fd.close()
	return lends
	
def remove_short(infname, outfname, outids, threshold):
	fd = open(infname)
	fd_w = open(outfname, 'w')
	fd_w_id = open(outids, 'w')
	for line in fd:
		tmp_l = line
		line = line.strip().split(' ')
		id = line[0]
		ld = int(len(line[1:]))
		if ld >= threshold:
			fd_w.write(tmp_l)
			fd_w_id.write(id + '\n')
	fd_w.close()
	fd.close()
	fd_w_id.close()	
	
def filter_used_cat_labels(inlabel, outlabel, inids):
	ids = set()
	fd = open(inids)
	for line in fd:
		line = line.strip()
		ids.add(int(line))
	fd.close()
	fd = open(inlabel)
	fd_w = open(outlabel, 'w')	
	for line in fd:
		tmp_l = line
		line = line.strip().split(' ')
		id = int(line[0])
		if id in ids:
			fd_w.write(tmp_l)
	fd_w.close()
	fd.close()

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
	for id in ids:
		if id not in labels:
			print id + ' not in '
	
def check_correctness(infeature, inlabel):
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
	if len(f_ids) != len(l_ids):
		print 'does not equal in length'
		return
	else:
		for i in range(len(f_ids)):
			if f_ids[i] != l_ids[i]:
				print 'does not equal '	

def make_svm_label_file(inlabel, outlabel):
	fd = open(inlabel)
	fd_w = open(outlabel, 'w')
	for line in fd:
		tl = line
		line = line.strip().split(' ')
		new_line = line[0] + ' ' + str(len(line[1:])) + ' ' + ' '.join(line[1:]) + '\n'
		fd_w.write(new_line)		
	fd_w.close()
	fd.close()
	
if __name__ == '__main__':
	lends = get_length()
	threshold = 10
	in_raw_feature_fname = 'query_bing_id_svm.txt'
	in_raw_label_fname = 'query_bing_id_filtered_categories.txt'
	in_used_cat_fname = 'used_tree_nodes.txt'
	
	out_removed_feature_fname = 'new_query_bing_id_svm_remove_short_less_10.txt'
	out_removed_id_fname = 'query_bing_id_svm_remove_short_less_10_ids.txt'
	out_removed_label_fname = 'new_query_bing_id_filtered_categories_remove_short_less_10.txt'
	
	final_feature_name = 'new_query_bing_features.txt'
	final_label_name = 'new_query_bing_labels.txt'
	final_final_label_name = 'new_new_query_bing_labels.txt'
	"""
	remove_short(in_raw_feature_fname, out_removed_feature_fname, out_removed_id_fname, threshold)
	filter_used_cat_labels(in_raw_label_fname, out_removed_label_fname, out_removed_id_fname)
	
	check_empty(out_removed_label_fname, in_used_cat_fname)
	check_correctness(out_removed_feature_fname, out_removed_label_fname)
	
	import os
	os.system('mv ' + out_removed_feature_fname + ' ' + final_feature_name)	
	"""
	make_svm_label_file(final_label_name, final_final_label_name)	
	#check_correctness(final_feature_name, final_final_label_name)
