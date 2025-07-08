from drepr.readers.prelude import read_source_json
from drepr.utils.attr_data import AttributeData
from drepr.utils.attr_data import AttributeData
from drepr.utils.attr_data import AttributeData
from drepr.writers.turtle_writer import TurtleWriter

def main(resource_table_0):
	resource_data_table_1 = read_source_json(resource_table_0)
	
	name_0_missing_values_2 = {""}
	url_1_missing_values_3 = {""}
	products_services_2_missing_values_4 = {""}
	category_3_missing_values_5 = {""}
	service_type_4_missing_values_6 = {""}
	field_5_missing_values_7 = {""}
	status_6_missing_values_8 = {""}
	oneliner_7_missing_values_9 = {""}
	photo_8_missing_values_10 = {""}
	logo_9_missing_values_11 = {""}
	name_0_1_missing_values_12 = {None}
	category_3_1_missing_values_13 = {None}
	service_type_4_1_missing_values_14 = {None}
	resource_data_dynres_name_0_1_26 = preprocess_0(resource_data_table_1)
	resource_data_dynres_category_3_1_43 = preprocess_1(resource_data_table_1)
	resource_data_dynres_service_type_4_1_60 = preprocess_2(resource_data_table_1)
	writer_61 = TurtleWriter({"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "owl": "http://www.w3.org/2002/07/owl#", "space": "https://space.isi.edu/ontology/", "dc": "http://purl.org/dc/elements/1.1/", "foaf": "http://xmlns.com/foaf/0.1/", "xsd": "http://www.w3.org/2001/XMLSchema#", "drepr": "https://purl.org/drepr/1.0/"})
	
	# Transform records of class 7
	start_62 = 1
	end_63 = 46
	for category_3_1_index_0_64 in range(start_62, end_63):
		category_3_1_value_0_65 = resource_data_dynres_category_3_1_43[category_3_1_index_0_64]
		category_3_1_value_1_66 = category_3_1_value_0_65[3]
		start_67 = 0
		end_68 = len(category_3_1_value_1_66)
		for category_3_1_index_2_69 in range(start_67, end_68):
			category_3_1_value_2_70 = category_3_1_value_1_66[category_3_1_index_2_69]
			if category_3_1_value_2_70 in category_3_1_missing_values_13:
				continue
			writer_61.begin_record("https://space.isi.edu/ontology/Category", category_3_1_value_2_70, False, False)
			
			# Retrieve value of data property: category_3
			category_3_index_0_71 = category_3_1_index_0_64
			category_3_value_0_72 = resource_data_table_1[category_3_index_0_71]
			category_3_value_1_73 = category_3_value_0_72[3]
			category_3_index_2_74 = category_3_1_index_2_69
			category_3_value_2_75 = category_3_value_1_73[category_3_index_2_74]
			if not (category_3_value_2_75 in category_3_missing_values_5):
				writer_61.write_data_property("http://www.w3.org/2000/01/rdf-schema#label", category_3_value_2_75, None)
			
			writer_61.end_record()
	
	# Transform records of class 8
	start_76 = 1
	end_77 = 46
	for service_type_4_1_index_0_78 in range(start_76, end_77):
		service_type_4_1_value_0_79 = resource_data_dynres_service_type_4_1_60[service_type_4_1_index_0_78]
		service_type_4_1_value_1_80 = service_type_4_1_value_0_79[4]
		start_81 = 0
		end_82 = len(service_type_4_1_value_1_80)
		for service_type_4_1_index_2_83 in range(start_81, end_82):
			service_type_4_1_value_2_84 = service_type_4_1_value_1_80[service_type_4_1_index_2_83]
			if service_type_4_1_value_2_84 in service_type_4_1_missing_values_14:
				continue
			writer_61.begin_record("https://space.isi.edu/ontology/Service", service_type_4_1_value_2_84, False, False)
			
			# Retrieve value of data property: service_type_4
			service_type_4_index_0_85 = service_type_4_1_index_0_78
			service_type_4_value_0_86 = resource_data_table_1[service_type_4_index_0_85]
			service_type_4_value_1_87 = service_type_4_value_0_86[4]
			service_type_4_index_2_88 = service_type_4_1_index_2_83
			service_type_4_value_2_89 = service_type_4_value_1_87[service_type_4_index_2_88]
			if not (service_type_4_value_2_89 in service_type_4_missing_values_6):
				writer_61.write_data_property("http://www.w3.org/2000/01/rdf-schema#label", service_type_4_value_2_89, None)
			
			writer_61.end_record()
	
	# Transform records of class 0
	start_90 = 1
	end_91 = 46
	for name_0_1_index_0_92 in range(start_90, end_91):
		name_0_1_value_0_93 = resource_data_dynres_name_0_1_26[name_0_1_index_0_92]
		name_0_1_value_1_94 = name_0_1_value_0_93[0]
		if name_0_1_value_1_94 in name_0_1_missing_values_12:
			continue
		writer_61.begin_record("https://space.isi.edu/ontology/Company", name_0_1_value_1_94, False, False)
		
		# Retrieve value of data property: name_0
		name_0_index_0_95 = name_0_1_index_0_92
		name_0_value_0_96 = resource_data_table_1[name_0_index_0_95]
		name_0_value_1_97 = name_0_value_0_96[0]
		if not (name_0_value_1_97 in name_0_missing_values_2):
			writer_61.write_data_property("http://www.w3.org/2000/01/rdf-schema#label", name_0_value_1_97, None)
		
		# Retrieve value of data property: url_1
		url_1_index_0_98 = name_0_1_index_0_92
		url_1_value_0_99 = resource_data_table_1[url_1_index_0_98]
		url_1_value_1_100 = url_1_value_0_99[1]
		if not (url_1_value_1_100 in url_1_missing_values_3):
			writer_61.write_data_property("https://space.isi.edu/ontology/url", url_1_value_1_100, "http://www.w3.org/2001/XMLSchema#anyURI")
		
		# Retrieve value of data property: products_services_2
		products_services_2_index_0_101 = name_0_1_index_0_92
		products_services_2_value_0_102 = resource_data_table_1[products_services_2_index_0_101]
		products_services_2_value_1_103 = products_services_2_value_0_102[2]
		if not (products_services_2_value_1_103 in products_services_2_missing_values_4):
			writer_61.write_data_property("https://space.isi.edu/ontology/product", products_services_2_value_1_103, "http://www.w3.org/2001/XMLSchema#string")
		
		# Retrieve value of data property: oneliner_7
		oneliner_7_index_0_104 = name_0_1_index_0_92
		oneliner_7_value_0_105 = resource_data_table_1[oneliner_7_index_0_104]
		oneliner_7_value_1_106 = oneliner_7_value_0_105[7]
		if not (oneliner_7_value_1_106 in oneliner_7_missing_values_9):
			writer_61.write_data_property("http://www.w3.org/2000/01/rdf-schema#comment", oneliner_7_value_1_106, "http://www.w3.org/2001/XMLSchema#string")
		
		# Retrieve value of data property: field_5
		field_5_index_0_107 = name_0_1_index_0_92
		field_5_value_0_108 = resource_data_table_1[field_5_index_0_107]
		field_5_value_1_109 = field_5_value_0_108[5]
		start_110 = 0
		end_111 = len(field_5_value_1_109)
		for field_5_index_2_112 in range(start_110, end_111):
			field_5_value_2_113 = field_5_value_1_109[field_5_index_2_112]
			if not (field_5_value_2_113 in field_5_missing_values_7):
				writer_61.write_data_property("https://space.isi.edu/ontology/field", field_5_value_2_113, "http://www.w3.org/2001/XMLSchema#string")
		
		# Retrieve value of data property: status_6
		status_6_index_0_114 = name_0_1_index_0_92
		status_6_value_0_115 = resource_data_table_1[status_6_index_0_114]
		status_6_value_1_116 = status_6_value_0_115[6]
		start_117 = 0
		end_118 = len(status_6_value_1_116)
		for status_6_index_2_119 in range(start_117, end_118):
			status_6_value_2_120 = status_6_value_1_116[status_6_index_2_119]
			if not (status_6_value_2_120 in status_6_missing_values_8):
				writer_61.write_data_property("https://space.isi.edu/ontology/status", status_6_value_2_120, "http://www.w3.org/2001/XMLSchema#string")
		
		# Retrieve value of object property: category_3_1
		category_3_1_index_0_121 = name_0_1_index_0_92
		category_3_1_value_0_122 = resource_data_dynres_category_3_1_43[category_3_1_index_0_121]
		category_3_1_value_1_123 = category_3_1_value_0_122[3]
		start_124 = 0
		end_125 = len(category_3_1_value_1_123)
		for category_3_1_index_2_126 in range(start_124, end_125):
			category_3_1_value_2_127 = category_3_1_value_1_123[category_3_1_index_2_126]
			if writer_61.has_written_record(category_3_1_value_2_127):
				writer_61.write_object_property("https://space.isi.edu/ontology/category", category_3_1_value_2_127, False, False, False)
		
		# Retrieve value of object property: service_type_4_1
		service_type_4_1_index_0_128 = name_0_1_index_0_92
		service_type_4_1_value_0_129 = resource_data_dynres_service_type_4_1_60[service_type_4_1_index_0_128]
		service_type_4_1_value_1_130 = service_type_4_1_value_0_129[4]
		start_131 = 0
		end_132 = len(service_type_4_1_value_1_130)
		for service_type_4_1_index_2_133 in range(start_131, end_132):
			service_type_4_1_value_2_134 = service_type_4_1_value_1_130[service_type_4_1_index_2_133]
			if writer_61.has_written_record(service_type_4_1_value_2_134):
				writer_61.write_object_property("https://space.isi.edu/ontology/service", service_type_4_1_value_2_134, False, False, False)
		
		# Set static properties
		writer_61.write_data_property("https://space.isi.edu/ontology/category", "http://space.isi.edu/resource/d1e17111c3ba4e8c08265c746430e4c02a355afecf8fa68a074f651e09c3e57a", "https://purl.org/drepr/1.0/uri")
		
		writer_61.end_record()
	
	output_135 = writer_61.write_to_string()
	return output_135

def preprocess_0(resource_data_15):
	name_0_1_18 = AttributeData.from_raw_path(["1..46:1", 0])
	start_19 = 1
	end_20 = 46
	for preproc_0_path_index_0_21 in range(start_19, end_20):
		preproc_0_path_value_0_22 = resource_data_15[preproc_0_path_index_0_21]
		name_0_1_value_0_23 = name_0_1_18[preproc_0_path_index_0_21]
		preproc_0_path_value_1_24 = preproc_0_path_value_0_22[0]
		name_0_1_value_1_25 = name_0_1_value_0_23[0]
		name_0_1_value_0_23[0] = preproc_0_customfn_17(preproc_0_path_value_1_24)
	return name_0_1_18

def get_preproc_0_customfn():
	from hashlib import sha256
	from urllib.parse import urljoin, urlparse
	def preproc_0_customfn(value):
		result = urlparse(value)
		if result.netloc != "" and result.scheme != "":
			return value
		return urljoin("http://space.isi.edu/resource/", sha256(value.encode()).hexdigest())
	return preproc_0_customfn

preproc_0_customfn_17 = get_preproc_0_customfn()

def preprocess_1(resource_data_27):
	category_3_1_30 = AttributeData.from_raw_path(["1..46:1", 3, "0..:1"])
	start_31 = 1
	end_32 = 46
	for preproc_1_path_index_0_33 in range(start_31, end_32):
		preproc_1_path_value_0_34 = resource_data_27[preproc_1_path_index_0_33]
		category_3_1_value_0_35 = category_3_1_30[preproc_1_path_index_0_33]
		preproc_1_path_value_1_36 = preproc_1_path_value_0_34[3]
		category_3_1_value_1_37 = category_3_1_value_0_35[3]
		start_38 = 0
		end_39 = len(preproc_1_path_value_1_36)
		for preproc_1_path_index_2_40 in range(start_38, end_39):
			preproc_1_path_value_2_41 = preproc_1_path_value_1_36[preproc_1_path_index_2_40]
			category_3_1_value_2_42 = category_3_1_value_1_37[preproc_1_path_index_2_40]
			category_3_1_value_1_37[preproc_1_path_index_2_40] = preproc_1_customfn_29(preproc_1_path_value_2_41)
	return category_3_1_30

def get_preproc_1_customfn():
	from hashlib import sha256
	from urllib.parse import urljoin, urlparse
	def preproc_1_customfn(value):
		result = urlparse(value)
		if result.netloc != "" and result.scheme != "":
			return value
		return urljoin("http://space.isi.edu/resource/", sha256(value.encode()).hexdigest())
	return preproc_1_customfn

preproc_1_customfn_29 = get_preproc_1_customfn()

def preprocess_2(resource_data_44):
	service_type_4_1_47 = AttributeData.from_raw_path(["1..46:1", 4, "0..:1"])
	start_48 = 1
	end_49 = 46
	for preproc_2_path_index_0_50 in range(start_48, end_49):
		preproc_2_path_value_0_51 = resource_data_44[preproc_2_path_index_0_50]
		service_type_4_1_value_0_52 = service_type_4_1_47[preproc_2_path_index_0_50]
		preproc_2_path_value_1_53 = preproc_2_path_value_0_51[4]
		service_type_4_1_value_1_54 = service_type_4_1_value_0_52[4]
		start_55 = 0
		end_56 = len(preproc_2_path_value_1_53)
		for preproc_2_path_index_2_57 in range(start_55, end_56):
			preproc_2_path_value_2_58 = preproc_2_path_value_1_53[preproc_2_path_index_2_57]
			service_type_4_1_value_2_59 = service_type_4_1_value_1_54[preproc_2_path_index_2_57]
			service_type_4_1_value_1_54[preproc_2_path_index_2_57] = preproc_2_customfn_46(preproc_2_path_value_2_58)
	return service_type_4_1_47

def get_preproc_2_customfn():
	from hashlib import sha256
	from urllib.parse import urljoin, urlparse
	def preproc_2_customfn(value):
		result = urlparse(value)
		if result.netloc != "" and result.scheme != "":
			return value
		return urljoin("http://space.isi.edu/resource/", sha256(value.encode()).hexdigest())
	return preproc_2_customfn

preproc_2_customfn_46 = get_preproc_2_customfn()

if __name__ == "__main__":
	import sys
	
	print(main(*sys.argv[1:]))