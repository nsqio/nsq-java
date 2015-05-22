package ly.bit.nsq.util;

public class StringUtils {
	public static String trimRight(String trimStr, String sourceStr) {
		if (trimStr == null || sourceStr == null
				|| trimStr.equals("") || sourceStr.equals("")) return sourceStr;
		
		int len = sourceStr.length();
		if (sourceStr.lastIndexOf(trimStr) == (len-1)) return sourceStr.substring(0, len-1);
		return sourceStr;
	}
	
	public static boolean isBlank(String str) {
		if (str == null) return true;
		if (str.trim().equals("")) return true;
		return false;
	}
}
