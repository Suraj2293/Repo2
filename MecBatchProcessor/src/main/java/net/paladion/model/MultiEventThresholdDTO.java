/**
 * 
 */
package net.paladion.model;

import java.sql.Timestamp;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.google.gson.annotations.SerializedName;

/**
 * @author ankush
 *
 */
public class MultiEventThresholdDTO {
	@SerializedName("eventTime")
	@Getter
	@Setter
	private Timestamp et;
	@SerializedName("matchedRuleId")
	@Getter
	@Setter
	private int mr;
	@SerializedName("threatId")
	@Getter
	@Setter
	private long ti;
	@SerializedName("applicationProtocol")
	@Getter
	@Setter
	private String aa;
	@SerializedName("baseEventCount")
	@Getter
	@Setter
	private int ab;
	@SerializedName("bytesIn")
	@Getter
	@Setter
	private int ac;
	@SerializedName("bytesOut")
	@Getter
	@Setter
	private int ad;
	@SerializedName("destinationAddress")
	@Getter
	@Setter
	private String ae;
	@SerializedName("destinationHostName")
	@Getter
	@Setter
	private String af;
	@SerializedName("destinationPort")
	@Getter
	@Setter
	private int ag;
	@SerializedName("destinationProcessName")
	@Getter
	@Setter
	private String ah;
	@SerializedName("destinationServiceName")
	@Getter
	@Setter
	private String ai;
	@SerializedName("destinationUserName")
	@Getter
	@Setter
	private String aj;
	@SerializedName("deviceAction")
	@Getter
	@Setter
	private String ak;
	@SerializedName("deviceAddress")
	@Getter
	@Setter
	private String al;
	@SerializedName("deviceCustomNumber1")
	@Getter
	@Setter
	private long am;
	@SerializedName("deviceCustomNumber2")
	@Getter
	@Setter
	private long an;
	@SerializedName("deviceCustomNumber3")
	@Getter
	@Setter
	private long ao;
	@SerializedName("deviceCustomString1")
	@Getter
	@Setter
	private String ap;
	@SerializedName("deviceCustomString2")
	@Getter
	@Setter
	private String aq;
	@SerializedName("deviceCustomString3")
	@Getter
	@Setter
	private String ar;
	@SerializedName("deviceCustomString4")
	@Getter
	@Setter
	private String da;
	@SerializedName("deviceCustomString5")
	@Getter
	@Setter
	private String at;
	@SerializedName("deviceCustomString6")
	@Getter
	@Setter
	private String au;
	@SerializedName("deviceDirection")
	@Getter
	@Setter
	private String av;
	@SerializedName("deviceEventCategory")
	@Getter
	@Setter
	private String aw;
	@SerializedName("deviceEventClassId")
	@Getter
	@Setter
	private String ax;
	@SerializedName("deviceHostName")
	@Getter
	@Setter
	private String ay;
	@SerializedName("deviceInboundInterface")
	@Getter
	@Setter
	private String az;
	@SerializedName("deviceOutboundInterface")
	@Getter
	@Setter
	private String ba;
	@SerializedName("deviceProcessName")
	@Getter
	@Setter
	private String bb;
	@SerializedName("deviceProduct")
	@Getter
	@Setter
	private String bc;
	@SerializedName("deviceReceiptTime")
	@Getter
	@Setter
	private Timestamp bd;
	@SerializedName("deviceSeverity")
	@Getter
	@Setter
	private String be;
	@SerializedName("deviceVendor")
	@Getter
	@Setter
	private String bf;
	@SerializedName("endTime")
	@Getter
	@Setter
	private Timestamp bg;
	@SerializedName("eventId")
	@Getter
	@Setter
	private long bh;
	@SerializedName("externalId")
	@Getter
	@Setter
	private String bi;
	@SerializedName("fileName")
	@Getter
	@Setter
	private String bj;
	@SerializedName("filePath")
	@Getter
	@Setter
	private String bk;
	@SerializedName("flexString1")
	@Getter
	@Setter
	private String bl;
	@SerializedName("message")
	@Getter
	@Setter
	private String bm;
	@SerializedName("name")
	@Getter
	@Setter
	private String bn;
	@SerializedName("reason")
	@Getter
	@Setter
	private String bo;
	@SerializedName("requestClientApplication")
	@Getter
	@Setter
	private String bp;
	@SerializedName("requestContext")
	@Getter
	@Setter
	private String bq;
	@SerializedName("requestCookie")
	@Getter
	@Setter
	private String br;
	@SerializedName("requestCookies")
	@Getter
	@Setter
	private String bs;
	@SerializedName("requestMethod")
	@Getter
	@Setter
	private String bt;
	@SerializedName("requestUrl")
	@Getter
	@Setter
	private String bu;
	@SerializedName("requestUrlFileName")
	@Getter
	@Setter
	private String bv;
	@SerializedName("sourceAddress")
	@Getter
	@Setter
	private String bw;
	@SerializedName("sourceHostName")
	@Getter
	@Setter
	private String bx;
	@SerializedName("sourceNtDomain")
	@Getter
	@Setter
	private String db;
	@SerializedName("sourcePort")
	@Getter
	@Setter
	private int dc;
	@SerializedName("sourceUserName")
	@Getter
	@Setter
	private String bz;
	@SerializedName("startTime")
	@Getter
	@Setter
	private Timestamp ca;
	@SerializedName("transportProtocol")
	@Getter
	@Setter
	private String cb;
	@SerializedName("categoryBehavior")
	@Getter
	@Setter
	private String cg;
	@SerializedName("categoryOutcome")
	@Getter
	@Setter
	private String ch;
	@SerializedName("customerURI")
	@Getter
	@Setter
	private String ci;
	@SerializedName("adrCustomStr1")
	@Getter
	@Setter
	private String cj;
	@SerializedName("adrCustomStr2")
	@Getter
	@Setter
	private String ck;
	@SerializedName("adrCustomStr3")
	@Getter
	@Setter
	private String cl;
	@SerializedName("adrCustomStr4")
	@Getter
	@Setter
	private String cm;
	@SerializedName("adrCustomeNumber1")
	@Getter
	@Setter
	private int cn;
	@SerializedName("customerName")
	@Getter
	@Setter
	private String cu;
	@SerializedName("severity")
	@Getter
	@Setter
	private String se;
	@SerializedName("recommendations")
	@Getter
	@Setter
	private String re;
	@SerializedName("clientTopic")
	@Setter
	@Getter
	private String ct;
	@SerializedName("others")
	@Getter
	@Setter
	private String co;
	@SerializedName("identicalColumn")
	@Getter
	@Setter
	private String ri;
	@SerializedName("distinctColumn")
	@Getter
	@Setter
	private String rd;
	@SerializedName("thresholdValue")
	@Getter
	@Setter
	private int rt;
	@SerializedName("timeWindowUnit")
	@Getter
	@Setter
	private String ru;
	@SerializedName("timeWindowValue")
	@Getter
	@Setter
	private int rv;
	@SerializedName("streamEntryDate")
	@Getter
	@Setter
	private Timestamp sd;
	@SerializedName("ruleName")
	@Getter
	@Setter
	private String rn;
	@SerializedName("rulerecommendations")
	@Getter
	@Setter
	private String rre;
	@SerializedName("ruleseverity")
	@Getter
	@Setter
	private String rs;
	@SerializedName("follwedByRuleName")
	@Getter
	@Setter
	private String rf;
	@SerializedName("sourceZone")
	@Getter
	@Setter
	private String sz;
	@SerializedName("destinationZone")
	@Getter
	@Setter
	private String dz;
	@SerializedName("sourceGeoCityName")
	@Getter
	@Setter
	private String cp;
	@SerializedName("sourceGeoCountryName")
	@Getter
	@Setter
	private String cq;
	@SerializedName("destinationGeoCityName")
	@Getter
	@Setter
	private String cr;
	@SerializedName("destinationGeoCountryName")
	@Getter
	@Setter
	private String cs;
	@SerializedName("eventTimeLong")
	@Getter
	@Setter
	private long el;
	@SerializedName("count")
	@Getter
	@Setter
	private long count;
	@SerializedName("childList")
	@Getter
	@Setter
	private List<Long> chl;
	@SerializedName("matchedRecords")
	@Getter
	@Setter
	private String matchedRecords;
	@SerializedName("matchedFamilyId")
	@Getter
	@Setter
	private long matchedFamilyId;
	@Getter
	@Setter
	private String topic;
	@Getter
	@Setter
	private Integer partition;
	@Getter
	@Setter
	private Long offset;

	public MultiEventThresholdDTO() {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MultiEventThresholdDTO [et=" + et + ", mr=" + mr + ", ti=" + ti
				+ ", aa=" + aa + ", ab=" + ab + ", ac=" + ac + ", ad=" + ad
				+ ", ae=" + ae + ", af=" + af + ", ag=" + ag + ", ah=" + ah
				+ ", ai=" + ai + ", aj=" + aj + ", ak=" + ak + ", al=" + al
				+ ", am=" + am + ", an=" + an + ", ao=" + ao + ", ap=" + ap
				+ ", aq=" + aq + ", ar=" + ar + ", da=" + da + ", at=" + at
				+ ", au=" + au + ", av=" + av + ", aw=" + aw + ", ax=" + ax
				+ ", ay=" + ay + ", az=" + az + ", ba=" + ba + ", bb=" + bb
				+ ", bc=" + bc + ", bd=" + bd + ", be=" + be + ", bf=" + bf
				+ ", bg=" + bg + ", bh=" + bh + ", bi=" + bi + ", bj=" + bj
				+ ", bk=" + bk + ", bl=" + bl + ", bm=" + bm + ", bn=" + bn
				+ ", bo=" + bo + ", bp=" + bp + ", bq=" + bq + ", br=" + br
				+ ", bs=" + bs + ", bt=" + bt + ", bu=" + bu + ", bv=" + bv
				+ ", bw=" + bw + ", bx=" + bx + ", db=" + db + ", dc=" + dc
				+ ", bz=" + bz + ", ca=" + ca + ", cb=" + cb + ", cg=" + cg
				+ ", ch=" + ch + ", ci=" + ci + ", cj=" + cj + ", ck=" + ck
				+ ", cl=" + cl + ", cm=" + cm + ", cn=" + cn + ", cu=" + cu
				+ ", se=" + se + ", re=" + re + ", ct=" + ct + ", co=" + co
				+ ", ri=" + ri + ", rd=" + rd + ", rt=" + rt + ", ru=" + ru
				+ ", rv=" + rv + ", sd=" + sd + ", rn=" + rn + ", rf=" + rf
				+ ", el=" + el + ", count=" + count + ", sz=" + sz + ", dz="
				+ dz + ", cp=" + cp + ", cq=" + cq + ", cr=" + cr + ", cs="
				+ cs + ", chl=" + chl + ", matchedRecords=" + matchedRecords
				+ ", matchedFamilyId=" + matchedFamilyId + ", topic=" + topic
				+ ", partition=" + partition + ", offset=" + offset + "]";
	}
}
