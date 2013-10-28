import javax.servlet.http.*;
import javax.servlet.*;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

public class UpdatePrediction extends HttpServlet
{
    static String template = "";
    
    static Connection conn = null;
	static String url = "jdbc:mysql://192.168.0.2:3306/";
	static String dbName = "seeu";
	static String driver = "com.mysql.jdbc.Driver";
	static String userName = "root"; 
	static String password = "see";
	static String tb = "comments";
	static String mobile_url = "http://kdd.csd.uwo.ca:88/seeumobile/index.html";
    static String pc_url = "http://kdd.csd.uwo.ca:88/seeu/index.html";    
    static String dest = "";
    
    public void init() throws ServletException {	
	     // The int parameters can also be retrieved using the servlet context
    	//probability prediction template
    	template = getServletConfig().getInitParameter("prediction_template");
    	//the junp webpage
    	dest = getServletConfig().getInitParameter("dest");
	 }
    
    /*
     * 
     * Call JAR package to update the data entry
     * (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
               throws ServletException,IOException
    {			
		String uname = req.getParameter("uname");
		String id = req.getParameter("id");
		String labels = req.getParameter("labels");//connected by underline
		String threshold = req.getParameter("threshold");		
		if(threshold == null)
			threshold = "0.6";
		
		if(uname == null || id == null || labels == null)
		{
			System.out.println("uname, id or labels are null");
			return;
		}
		
		//build the prediction list based on the labels inputed
		HashMap<String, String> prediction = new HashMap<String, String>();
		String [] cat_lst = labels.split("_");
		for(int i=0;i<cat_lst.length;i++)
			prediction.put(cat_lst[i], threshold);		
				
		//make the initial prediction for other label from template
		String [] label_lst = template.split(",");		
		ArrayList<String> cat_names = new ArrayList<String>();
		for(int i=0;i<label_lst.length;i++)
		{
			String [] label_data = label_lst[i].split(":");
			cat_names.add(label_data[0]);			
			if(prediction.containsKey(label_data[0]) == false)
				prediction.put(label_data[0], "0.000");//initialize the probability estimation for this node is zero			
		}
		
		//make data as the prediction format
		String output_str = "";
		for(int i=0;i<label_lst.length;i++)
		{
			if(i < label_lst.length - 1)
				output_str += cat_names.get(i) + ":" + prediction.get(cat_names.get(i)) + ",";
			else
				output_str += cat_names.get(i) + ":" + prediction.get(cat_names.get(i));
		}
		
		//update data in the database
		String msg = "done";
		
		try{
			dbName = uname;
			
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url+dbName,userName,password);				
				
			//insert into tb
			PreparedStatement pStmt = conn.prepareStatement("update new_prediction set prediction=? where id=" + id);
			pStmt.setString(1, output_str);					    
		    pStmt.executeUpdate();					 
		    
			conn.close();
			 
			resp.sendRedirect(dest);
			
		} catch (Exception e) {
			System.out.println("database error");
			e.printStackTrace();
			msg = "Error";	
			
			java.io.PrintWriter pw=resp.getWriter();
			pw.write(msg);								
			pw.close();
		}								  
    }
	
	static public void test()
	{
		/*
		String uname = req.getParameter("uname");
		String id = req.getParameter("id");
		String labels = req.getParameter("labels");//connected by underline
		String threshold = req.getParameter("threshold");		
		if(threshold == null)
			threshold = "0.6";
		*/
		
		String uname = "uwestern";
		String id = "0";
		String labels = "1";//connected by underline
		String threshold = "0.6";
		
		if(uname == null || id == null || labels == null)
		{
			System.out.println("uname, id or labels are null");
			return;
		}
		
		//build the prediction list based on the labels inputed
		HashMap<String, String> prediction = new HashMap<String, String>();
		String [] cat_lst = labels.split("_");
		for(int i=0;i<cat_lst.length;i++)
			prediction.put(cat_lst[i], threshold);		
				
		//make the initial prediction for other label from template
		template = "0:0.00032,1:9e-05,2:0.00226,3:0.00312,4:8e-05,5:0.00499,6:0.00223,7:9e-05,8:0.00027,9:0.00034,10:9e-05,21:4e-05,22:0.14186,28:0.00217,29:0.00555,30:0.00031,31:0.00047,32:0.00122,33:0.01098,34:0.00717,35:0.00089,36:0.01665,38:0.00034,43:0.00028,46:0.00237,47:0.00029,48:0.18618,66:0.07304,71:0.10869,73:0.0131,78:0.00895,89:0.0693,91:0.05231,95:0.12692,96:0.01948,97:0.75002,98:0.00412,99:0.01193,100:0.57107,101:0.00398,102:0.00533,103:0.008,104:0.00045,105:0.99298,108:0.60712,109:0.05449,112:0.0013,113:0.00224,114:0.00021,122:0.00452,125:0.00833,126:0.00429,127:0.00127,128:0.0053,129:0.12621,130:0.00583,131:0.08676,132:0.0007,133:0.00072,134:0.00176,135:0.00249,137:0.00304,138:0.19404,143:0.00055,152:0.31782,157:0.001,162:0.0,164:0.00235,165:0.03344,167:0.02953,171:0.00574,172:0.70752,173:0.0001,174:0.00893,175:0.3581,176:0.0254,177:0.44439,178:0.01971,179:0.70299,183:0.00478,188:0.07989,191:0.06156,192:0.08975,193:0.9793,194:0.00123,195:0.01151,196:0.272,197:0.00508,198:0.41658,200:0.06971,201:0.00615,203:0.05217,204:0.00534,224:0.07309,225:0.06294,237:0.00574,238:0.00537,239:0.00203,240:0.00052,245:0.10428,246:0.06372,247:0.00039,252:0.06097,253:0.99998,265:0.0002,266:0.00828,267:0.00166,268:0.10215,269:9e-05,270:0.00049,271:6e-05,272:0.00669,273:0.00032,280:0.00696,281:0.60299,282:0.88392,283:0.00303,284:0.01132,285:0.00094,287:0.38001,288:0.9751,290:0.64029,291:0.0098,292:0.27156,296:0.00555,297:0.00311,298:0.00294,299:0.00185,300:0.00152,301:0.00267,331:0.00012,332:0.00357,333:0.16193,336:0.00577,337:0.00287,341:0.0022,342:0.07487,343:0.00375,344:0.00264,345:0.03057,346:0.08012,347:0.00802,348:0.40873,349:0.35219,350:0.01999,351:0.42548,352:0.87909,356:0.04907,357:0.25701,358:0.00033,359:0.04314,360:0.00154,361:0.02882,362:0.9622,363:0.01908,364:0.99976,368:0.00015,369:0.0026,370:0.00072,372:0.00265,385:0.00257,386:0.00989,387:0.01645,389:0.00693,394:0.04915,395:0.96128,397:0.26337,401:0.00082,403:0.58618,404:0.00105,405:0.97801,406:0.00372,414:0.05305,415:0.00365,417:0.00628,418:0.98402,425:0.00562,426:0.0065,430:0.00052,431:0.0001,432:0.03524,433:0.16968,434:0.00103,435:0.0169,436:0.02914,437:0.00084,438:0.00829,439:0.01139,440:0.00153,441:0.00403,442:0.0088,443:0.01494,444:0.00343,456:0.00034,457:0.00164,458:0.01493,460:0.00358,461:0.11241,464:0.00205,465:0.00674,466:0.05179,467:0.00156,469:0.00683,470:0.00753,471:0.00339,472:0.01594,473:0.00783,474:0.86442,475:0.01587,476:0.31698,477:0.00092,478:0.01506,479:0.00542,480:0.99362,481:0.12811,482:0.58713,483:0.9303,484:0.00437,485:0.0153,486:0.60714,487:0.00226,488:0.00172,489:0.00377,491:0.00088,492:0.00145,493:0.00242,494:0.01549,495:0.00875,501:0.00182,502:0.00128,504:0.0026,505:8e-05,507:0.00019,508:4e-05,509:0.00777,510:0.00148,511:0.00042,512:0.00162,513:0.17878,516:0.00269,518:0.03637,521:0.29448,525:0.0011,526:0.00048,527:0.02453,528:0.0007,530:0.00978,532:0.00054,534:0.00012,537:0.0004,539:4e-05,540:0.00026,543:0.00074,544:0.00126,545:0.00735,546:0.00705,547:0.01154,548:0.00577,550:0.06025,551:0.00707,553:0.00149,554:6e-05,555:0.01268,556:0.15478,557:3e-05,561:0.00164,564:0.02074,565:0.00292,566:0.02511,569:0.3955,572:0.82445,573:0.00264,581:0.00052,588:0.07024,594:5e-05,595:0.05285,596:0.00123,597:0.00065,598:0.00065,599:0.34218,600:0.40041,601:0.18648,603:0.00311,604:0.00246,607:0.02006,611:0.01263,613:0.00548,621:0.00078,625:0.06906,626:0.90172,628:0.16493,629:0.85687,633:0.0001,636:0.0004,638:0.00023,641:0.89845,648:0.01859,649:0.54888,650:0.00939,651:0.07478,670:0.16834,671:0.06041,691:0.39146,703:0.0198,708:0.19483,722:0.00059,726:7e-05,733:0.00066,734:0.10603,735:0.13227,736:0.04075,748:0.00052,751:0.01122,759:0.00396,761:0.00082,762:0.01318,763:0.00129,764:0.00063,765:0.88482,770:0.00153,773:0.00714,775:0.04837,783:0.02048,788:0.00018,789:0.00323,790:0.01627,799:0.0882,804:0.00635,815:0.00648,816:4e-05,817:0.00032,818:0.00026,819:0.00136,820:0.01447,821:0.14189,822:0.00094,839:0.00013,840:0.52421,841:0.00566,842:0.00268,843:0.00695,844:0.01736,845:0.00981,846:0.03742,847:0.3961,848:0.00607,849:0.00285,853:0.00135,854:0.15971,858:0.34566,860:0.02192,864:0.00463,867:0.00077,868:0.00567,870:0.00053,874:0.00102,877:0.02194,878:0.01081,880:0.00834,883:0.00381,885:0.00442,887:0.00368,888:0.00184,891:0.0026,894:0.00798,895:0.00349,896:0.03923,897:0.10509,898:0.03677,902:0.01152,904:0.02371,906:0.99246,907:1e-05,908:0.00068,909:0.00135,910:0.45532,912:0.07991,914:0.01035,915:0.37205,916:0.73938,919:0.01384,920:0.30689,925:0.0121,926:0.30325,927:0.25852,928:0.00095,929:0.01408,931:0.04646,932:0.00578,933:0.03887,934:0.33445,938:0.00041,940:0.32355,941:0.01673,942:0.71439,943:0.76982,944:0.00527,945:0.08524,946:0.00117,947:0.00555,948:0.24014,950:0.00294,951:0.23714,952:0.00579,953:0.00508,954:0.01705,956:0.08293,958:0.00408,962:0.01475,963:0.05194,964:0.00085,965:0.00056,967:0.00024,968:0.01423,969:0.24439,972:0.03477,973:0.0057,977:0.00228,982:0.00636,987:0.13598,989:0.00559,991:0.00338,992:0.00624,998:0.32866,999:0.0008,1000:0.00305,1001:0.00491,1002:0.00032,1007:0.43871,1009:0.00115,1011:0.00042,1012:0.0128,1013:0.01702,1014:0.14561,1015:0.0005,1019:0.15734,1020:0.00849,1027:0.00339,1031:0.01422,1037:0.17243,1053:0.0013,1058:0.00534,1060:0.00017,1061:0.00194,1072:0.00615,1081:0.00327,1082:0.01608,1086:0.01361,1087:0.00703,1091:0.00295,1094:0.01201,1095:0.56999,1102:0.02713,1104:0.00276,1106:0.00195,1107:0.00563,1115:0.00097,1123:0.00202,1125:0.00271,1126:0.00848,1127:0.00866,1128:0.10586,1129:0.00102,1130:0.00151,1131:0.01303,1134:0.0,1136:0.00084,1138:0.00502,1145:0.09903,1159:4e-05,1162:0.00026,1163:0.00149,1164:0.01234,1165:0.00254,1166:0.0015,1167:0.01339,1168:0.16089,1170:0.3776,1171:0.0038,1178:0.02419,1179:0.02553,1180:0.01756,1181:5e-05,1182:0.00068,1187:0.78716,1194:0.70363,1209:4e-05,1210:0.00664,1211:0.00128,1216:0.00011,1219:0.06593,1224:0.29347,1226:0.07843,1227:0.03828";
		String [] label_lst = template.split(",");		
		ArrayList<String> cat_names = new ArrayList<String>();
		for(int i=0;i<label_lst.length;i++)
		{
			String [] label_data = label_lst[i].split(":");
			cat_names.add(label_data[0]);
			System.out.println(label_data[0] + " " + label_data[1]);
			if(prediction.containsKey(label_data[0]) == false)
				prediction.put(label_data[0], "0.000");//initialize the probability estimation for this node is zero			
		}
		
		//make data as the prediction format
		String output_str = "";
		for(int i=0;i<label_lst.length;i++)
		{
			if(i < label_lst.length - 1)
				output_str += cat_names.get(i) + ":" + prediction.get(cat_names.get(i)) + ",";
			else
				output_str += cat_names.get(i) + ":" + prediction.get(cat_names.get(i));
		}
		
		//update data in the database
		String msg = "done";
		
		try{
			dbName = uname;
			
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url+dbName,userName,password);				
				
			//insert into tb
			PreparedStatement pStmt = conn.prepareStatement("update new_prediction set prediction=? where id=" + id);
			pStmt.setString(1, output_str);					    
		    pStmt.executeUpdate();
			
		    System.out.println(pStmt.toString());
		    
			conn.close();
			 			
		} catch (Exception e) {
			System.out.println("database error");
			e.printStackTrace();
			msg = "Error";				
		}	
	}
	
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException,IOException
    {
        doGet(req,resp);
    }	
    
    static public void main(String [] argv)
    {
    	UpdatePrediction.test();
    }
}