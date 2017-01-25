/*
 * Author: Keerthi Teja Konuri
 * Net id: kxk154530
 * Title: Files & Indexing
 */



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

class Record {
	int id;
	String company;
	char[] drugid;
	short trials, patients, dosage_mg;
	float reading;
	boolean double_blind, controlled_study, govt_funded, fda_approved, exists;

	byte flags;

	public Record(String[] stringArray) {
		this.id = Integer.parseInt(stringArray[0]);
		stringArray[1] = stringArray[1].replace("\"", "");
		this.company = stringArray[1];
		this.drugid = new char[6];
		this.drugid = stringArray[2].toCharArray();
		this.trials = Short.parseShort(stringArray[3]);
		this.patients = Short.parseShort(stringArray[4]);
		this.dosage_mg = Short.parseShort(stringArray[5]);
		this.reading = Float.parseFloat(stringArray[6]);

		if (stringArray[7].equalsIgnoreCase("TRUE"))
			flags = (byte) (flags | 8);
		if (stringArray[8].equalsIgnoreCase("TRUE"))
			flags = (byte) (flags | 4);
		if (stringArray[9].equalsIgnoreCase("TRUE"))
			flags = (byte) (flags | 2);
		if (stringArray[10].equalsIgnoreCase("TRUE"))
			flags = (byte) (flags | 1);
      //if(stringArray[11].equalsIgnoreCase("TRUE"))
			//flags = (byte) (flags | 1);

		this.double_blind = Boolean.parseBoolean(stringArray[7]);
		this.controlled_study = Boolean.parseBoolean(stringArray[8]);
		this.govt_funded = Boolean.parseBoolean(stringArray[9]);
		this.fda_approved = Boolean.parseBoolean(stringArray[10]);
	   this.exists = true;
      //this.exists = Boolean.parseBoolean(stringArray[11]);
   }

	public Record(int id, String company, char[] drugid, short trials, short patients,
			short dosage_mg, float reading, boolean double_blind, boolean controlled_study,
			boolean govt_funded, boolean fda_approved, boolean exists) {
		super();
		this.id = id;
		this.company = company;
		this.drugid = drugid;
		this.trials = trials;
		this.patients = patients;
		this.dosage_mg = dosage_mg;
		this.reading = reading;
		this.double_blind = double_blind;
		this.controlled_study = controlled_study;
		this.govt_funded = govt_funded;
		this.fda_approved = fda_approved;
      this.exists = exists;
	}

	@Override
	public String toString() {
		return "Record [" + id + ", " + company + ", " + new String(drugid) + ", " + trials + ", "
				+ patients + ", " + dosage_mg + ", " + reading + ", " + double_blind + ", "
				+ controlled_study + ", " + govt_funded + ", " + fda_approved +", "+ exists + "]";
	}

	public void display() {
		System.out.println(this.toString());
	}

	public Record() {
	}
   
}

public class MyDatabase {

	static final File INPUT_FILE = new File("PHARMA_TRIALS_1000B.csv");
	static final File DB_FILE = new File("data.db");

	static final File ID_INDEX_FILE = new File("id.ndx");
	static final File COMPANY_INDEX_FILE = new File("company.ndx");
	static final File TRIALS_INDEX_FILE = new File("trials.ndx");
	static final File PATIENTS_INDEX_FILE = new File("patients.ndx");
	static final File DOSAGE_INDEX_FILE = new File("dosage_mg.ndx");
	static final File READING_INDEX_FILE = new File("reading.ndx");
	static final File DOUBLEBLIND_INDEX_FILE = new File("double_blind.ndx");
	static final File CONTROLLED_STUDY_INDEX_FILE = new File("controlled_study.ndx");
	static final File GOVT_FUNDED_INDEX_FILE = new File("govt_funded.ndx");
	static final File FDA_APPROVED_INDEX_FILE = new File("fda_approved.ndx");
	static final File DRUG_ID_INDEX_FILE = new File("drug_id.ndx");
   static final File exists_INDEX_FILE = new File("exists.ndx");

	static TreeMap<Integer, Integer> idIndexMap = new TreeMap<Integer, Integer>();
	static TreeMap<String, TreeSet<Integer>> companyIndexMap = new TreeMap<String, TreeSet<Integer>>();
	static TreeMap<Short, TreeSet<Integer>> trialsIndexMap = new TreeMap<Short, TreeSet<Integer>>();
	static TreeMap<Short, TreeSet<Integer>> patientsIndexMap = new TreeMap<Short, TreeSet<Integer>>();
	static TreeMap<Short, TreeSet<Integer>> dosageIndexMap = new TreeMap<Short, TreeSet<Integer>>();
	static TreeMap<Float, TreeSet<Integer>> readingIndexMap = new TreeMap<Float, TreeSet<Integer>>();

	static TreeMap<Boolean, TreeSet<Integer>> doubleBlindIndexMap = new TreeMap<Boolean, TreeSet<Integer>>();
	static TreeMap<Boolean, TreeSet<Integer>> controlledStudyIndexMap = new TreeMap<Boolean, TreeSet<Integer>>();
	static TreeMap<Boolean, TreeSet<Integer>> govtFundedIndexMap = new TreeMap<Boolean, TreeSet<Integer>>();
	static TreeMap<Boolean, TreeSet<Integer>> fdaApprovedIndexMap = new TreeMap<Boolean, TreeSet<Integer>>();
   static TreeMap<Boolean, TreeSet<Integer>> existsIndexMap = new TreeMap<Boolean, TreeSet<Integer>>();

	static TreeMap<String, TreeSet<Integer>> drugIdIndexMap = new TreeMap<String, TreeSet<Integer>>();

	public static void parseInputFile() {

		try {
			BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE));
			String line;
			line = br.readLine(); // skip header line

			int count = 0;
			while ((line = br.readLine()) != null) {
				String[] stringArray = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				Record record = new Record(stringArray);
				writeToDB(record);
				count++;
			}
			updateIndexFiles();
			br.close();
		} catch (IOException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("IO Exception");
			System.exit(0);
		}
	}

	private static void updateIndexFiles() {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(ID_INDEX_FILE));
			for (Map.Entry<Integer, Integer> entry : idIndexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey() + " " + entry.getValue() + "\n");
				bw.write(line.toString());				
			}
			bw.close();

			bw = new BufferedWriter(new FileWriter(COMPANY_INDEX_FILE));
			for (Map.Entry<String, TreeSet<Integer>> entry : companyIndexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey());
				TreeSet<Integer> treeSet = entry.getValue();
				for (int address : treeSet)
					line.append("\t" + address);
				line.append("\n");
				bw.write(line.toString());
			}
			bw.close();

			updateShortIndexFiles(TRIALS_INDEX_FILE, trialsIndexMap);
			updateShortIndexFiles(PATIENTS_INDEX_FILE, patientsIndexMap);
			updateShortIndexFiles(DOSAGE_INDEX_FILE, dosageIndexMap);

			bw = new BufferedWriter(new FileWriter(READING_INDEX_FILE));
			for (Map.Entry<Float, TreeSet<Integer>> entry : readingIndexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey());
				TreeSet<Integer> treeSet = entry.getValue();
				for (int address : treeSet)
					line.append(" " + address);
				line.append("\n");
				bw.write(line.toString());
			}
			bw.close();

			updateBooleanIndexFiles(DOUBLEBLIND_INDEX_FILE, doubleBlindIndexMap);
			updateBooleanIndexFiles(CONTROLLED_STUDY_INDEX_FILE, controlledStudyIndexMap);
			updateBooleanIndexFiles(GOVT_FUNDED_INDEX_FILE, govtFundedIndexMap);
			updateBooleanIndexFiles(FDA_APPROVED_INDEX_FILE, fdaApprovedIndexMap);
         updateBooleanIndexFiles(exists_INDEX_FILE, existsIndexMap);

			bw = new BufferedWriter(new FileWriter(DRUG_ID_INDEX_FILE));
			for (Map.Entry<String, TreeSet<Integer>> entry : drugIdIndexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey());
				TreeSet<Integer> treeSet = entry.getValue();
				for (int address : treeSet)
					line.append("\t" + address);
				line.append("\n");
				bw.write(line.toString());
			}
			bw.close();
			loadIndexFiles();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void updateBooleanIndexFiles(File indexFile, TreeMap<Boolean, TreeSet<Integer>> indexMap) {
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(indexFile));
			for (Map.Entry<Boolean, TreeSet<Integer>> entry : indexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey());
				TreeSet<Integer> treeSet = entry.getValue();
				for (int address : treeSet)
					line.append(" " + address);
				line.append("\n");
				bw.write(line.toString());
			}
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void updateShortIndexFiles(File indexFile, TreeMap<Short, TreeSet<Integer>> indexMap) {
		
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(indexFile));
			for (Map.Entry<Short, TreeSet<Integer>> entry : indexMap.entrySet()) {
				StringBuilder line = new StringBuilder("");
				line.append(entry.getKey());
				TreeSet<Integer> treeSet = entry.getValue();
				for (int address : treeSet)
					line.append(" " + address);
				line.append("\n");
				bw.write(line.toString());
			}
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
   private static void updateDB(int baseAddress){
     
      
   }
   
	private static int writeToDB(Record r) {
		int baseAddress = -1;
		try {
			RandomAccessFile file = new RandomAccessFile(DB_FILE, "rw");
			file.seek(file.length());
			baseAddress = (int) file.getFilePointer();
         if (idIndexMap.containsKey(r.id))
         {
         System.out.println(" ID already exists enter a new Record to insert");
         }
         else
         {
			file.writeInt(r.id);
			file.writeByte(r.company.length());
			file.write(r.company.getBytes());

			for (char c : r.drugid)
				file.write((byte) c);

			file.writeShort(r.trials);
			file.writeShort(r.patients);
			file.writeShort(r.dosage_mg);
			file.writeFloat(r.reading);
			file.writeByte(r.flags);
        
         file.writeBoolean(r.exists);
			file.close();

			updateIndexMaps(baseAddress, r);
         }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return baseAddress;
	}

	private static void updateIndexMaps(int baseAddress, Record r) {

		idIndexMap.put(r.id, baseAddress);
		if (companyIndexMap.containsKey(r.company)) {
			companyIndexMap.get(r.company).add(baseAddress);
		} else {
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAddress);
			companyIndexMap.put(r.company, set);
		}
		updateShortIndexMaps(r.trials, trialsIndexMap, baseAddress);
		updateShortIndexMaps(r.patients, patientsIndexMap, baseAddress);
		updateShortIndexMaps(r.dosage_mg, dosageIndexMap, baseAddress);

		if (readingIndexMap.containsKey(r.reading)) {
			readingIndexMap.get(r.reading).add(baseAddress);
		} else {
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAddress);
			readingIndexMap.put(r.reading, set);
		}
		updateBooleanIndexMaps(r.double_blind, doubleBlindIndexMap, baseAddress);
		updateBooleanIndexMaps(r.controlled_study, controlledStudyIndexMap, baseAddress);
		updateBooleanIndexMaps(r.govt_funded, govtFundedIndexMap, baseAddress);
		updateBooleanIndexMaps(r.fda_approved, fdaApprovedIndexMap, baseAddress);
      updateBooleanIndexMaps(r.exists, existsIndexMap, baseAddress);

		if (drugIdIndexMap.containsKey(String.valueOf(r.drugid))) {
			drugIdIndexMap.get(String.valueOf(r.drugid)).add(baseAddress);
		} else {
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAddress);
			drugIdIndexMap.put(String.valueOf(r.drugid), set);
		}
	}

	private static void updateBooleanIndexMaps(boolean booleanValue,
			TreeMap<Boolean, TreeSet<Integer>> indexMap, int baseAddress) {
		if (indexMap.containsKey(booleanValue)) {
			indexMap.get(booleanValue).add(baseAddress);
		} else {
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAddress);
			indexMap.put(booleanValue, set);
		}

	}

	public static void updateShortIndexMaps(short shortValue,
			TreeMap<Short, TreeSet<Integer>> indexMap, int baseAddress) {
		if (indexMap.containsKey(shortValue)) {
			indexMap.get(shortValue).add(baseAddress);
		} else {
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAddress);
			indexMap.put(shortValue, set);
		}
	}

	public static boolean createDB() {
		boolean flag = false;
		try {
			if (DB_FILE.createNewFile()) {
				flag = true;
				System.out.println("Database File Created");
				createIndexFiles();
				parseInputFile();

			} else
				flag = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;
	}

	public static void createIndexFiles() {
		try {
			ID_INDEX_FILE.createNewFile();
			COMPANY_INDEX_FILE.createNewFile();
			TRIALS_INDEX_FILE.createNewFile();
			PATIENTS_INDEX_FILE.createNewFile();
			DOSAGE_INDEX_FILE.createNewFile();
			READING_INDEX_FILE.createNewFile();
			DOUBLEBLIND_INDEX_FILE.createNewFile();
			GOVT_FUNDED_INDEX_FILE.createNewFile();
			FDA_APPROVED_INDEX_FILE.createNewFile();
			CONTROLLED_STUDY_INDEX_FILE.createNewFile();
         exists_INDEX_FILE.createNewFile();
			DRUG_ID_INDEX_FILE.createNewFile();

			System.out.println("Index Files Are Created");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		if (dbExist()) {
			loadIndexFiles();
		}
		while (true) {
			int choice = printMenuOptions();
			switch (choice) {
			case 1:
				if (dbExist()) {
					System.out.println("Input File Is Already Parsed");
				}
				else
				{
					if(INPUT_FILE.exists())
					createDB();
					else
						System.out.println("Input File  PHARMA_TRIALS_1000B.csv Is Not Available");
				}
				break;
			case 2:
				if (dbExist())
					queryFields();
				else
					System.out.println("Input File Is Not Yet Parsed. Select Option 1 before querying fields");
				break;
         case 3:  Scanner sc = new Scanner(System.in);
                  System.out.println("Enter new record to Insert into Database");
                  int id;
                  while(true){
                  System.out.println("Enter Value for ID:");
                   id = sc.nextInt();
                     if (idIndexMap.containsKey(id)){
                        System.out.println(" ID already exists enter a new Record to insert");
                     }
                     else
                     break;
                  }
                  System.out.println("Enter Name of Company:");
                  String company = sc.next();
                  System.out.println("Enter Value for Drug_ID:");
                  String s_drug_id = sc.next();
                  char[] drug_id = new char[s_drug_id.length()];
                     for(int i = 0; i < s_drug_id.length(); i++)
                     drug_id [i] = s_drug_id.charAt(i);
                  System.out.println("Enter no. of Trials:");
                  short trials = sc.nextShort();
                  System.out.println("Enter no. of Patients:");
                  short patients = sc.nextShort();
                  System.out.println("Enter Dosage in mg:");
                  short dosage_mg = sc.nextShort();
                  System.out.println("Enter Reading:");
                  float reading = sc.nextFloat();
                  System.out.println("Is Double_Blind Y/N :");
                  String boolean_value1 = sc.next();
                  boolean double_blind , controlled_study, govt_funded, fda_approved;
                     if(boolean_value1.charAt(0) == 'y' || boolean_value1.charAt(0) == 'Y')
                     double_blind = true;
                     else
                     double_blind = false;
                  System.out.println("Is Contolled_Study Y/N :");
                  String boolean_value2 = sc.next();
                     if(boolean_value2.charAt(0) == 'y' || boolean_value2.charAt(0) == 'Y')
                     controlled_study = true;
                     else
                     controlled_study = false;
                  System.out.println("Is Govt_Handled Y/N :");
                  String boolean_value3 = sc.next();
                     if(boolean_value3.charAt(0) == 'y' || boolean_value3.charAt(0) == 'Y')
                     govt_funded = true;
                     else
                     govt_funded = false;
                  System.out.println("Is Fda_Aprroved Y/N :");
                  String boolean_value4 = sc.next();
                     if(boolean_value4.charAt(0) == 'y' || boolean_value4.charAt(0) == 'Y')
                     fda_approved = true;
                     else
                     fda_approved = false;
                     
                     Record newRecord = new Record(id,company,drug_id,trials,patients,dosage_mg,reading,double_blind,controlled_study,govt_funded,fda_approved,true);
                     //System.out.println("New Record Created");
                     int check =writeToDB(newRecord);
                     System.out.println("in insert"+check);
                     //System.out.println("Written to DB");
                     updateIndexFiles();
                     //System.out.println("Index Files updated");
                  break;
         case 4:
            if (dbExist())
					deleteRow();
				else
					System.out.println("Input File Is Not Yet Parsed. Select Option 1 before deleting a row");
                  break;
			case 5:
				System.out.println("Program Terminated");
				System.exit(0);
				break;
			}
		}
	}
   private static void deleteRow()
   {
      System.out.println("\n\n Enter the field, using with which we delete a row");
      System.out.println("1. ID");
		System.out.println("2. Company");
		System.out.println("3. Drug Id");
		System.out.println("4. Trials");
		System.out.println("5. Patients");
		System.out.println("6. Dosage_mg");
		System.out.println("7. Reading");
		System.out.println("8. Double_blind");
		System.out.println("9. Controlled Study");
		System.out.println("10.Government Funded");
		System.out.println("11. FDA Approved");
		System.out.println("0. ABORT OPERATION");
		Scanner sc = new Scanner(System.in);
		int choice = 0;
		while (true) {
			try {
				choice = sc.nextInt();
			} catch (InputMismatchException e) {
				System.out.println("Please enter integer");
				sc.next();
				continue;
			}
			if (choice > 11 || choice < 0)
				System.out.println("Please enter choice between 0-11");
			else
				break;
		}
         int comparison = 0;
		switch (choice) {
		case 0:
			break;
		case 1:
			System.out.println("Enter id number: ");
			int id = 0;
			while (true) {
				try {
					id = sc.nextInt();
				} catch (InputMismatchException e) {
					System.out.println("Please enter integer");
					sc.next();
					continue;
				}
				break;
			}
			comparison = getCompareChoice();
			if (comparison == 0)
				break;
         deleteRecordById(id, comparison);
         break;
      case 2:
			deleteStringRecords(companyIndexMap);
			break;
		case 3:
			deleteStringRecords(drugIdIndexMap);
			break;
		case 4:
			deleteShortRecords(trialsIndexMap);
			break;
		case 5:
			deleteShortRecords(patientsIndexMap);
			break;
		case 6:
			deleteShortRecords(dosageIndexMap);
			break;
		case 7:
			System.out.println("Enter reading value: ");
			Float readingValue = 0.0f;

			while (true) {
				try {
					readingValue = sc.nextFloat();
				} catch (InputMismatchException e) {
					System.out.println("Please enter float value");
					sc.next();
					continue;
				}
				break;
			}
			comparison = getCompareChoice();
			if (comparison == 0)
				break;
			deleteRecordsByReading(readingValue, comparison);
			break;
		case 8:
			deleteBooleanRecords(doubleBlindIndexMap);
			break;
		case 9:
			deleteBooleanRecords(controlledStudyIndexMap);
			break;
		case 10:
			deleteBooleanRecords(govtFundedIndexMap);
			break;
		case 11:
			deleteBooleanRecords(fdaApprovedIndexMap);
			break;
      }
   }
	private static void queryFields() {
		System.out.println("\n\nWhich field would you like to query?");
		System.out.println("1. ID");
		System.out.println("2. Company");
		System.out.println("3. Drug Id");
		System.out.println("4. Trials");
		System.out.println("5. Patients");
		System.out.println("6. Dosage_mg");
		System.out.println("7. Reading");
		System.out.println("8. Double_blind");
		System.out.println("9. Controlled Study");
		System.out.println("10.Government Funded");
		System.out.println("11. FDA Approved");
		System.out.println("0. ABORT OPERATION");
		Scanner sc = new Scanner(System.in);
		int choice = 0;
		while (true) {
			try {
				choice = sc.nextInt();
			} catch (InputMismatchException e) {
				System.out.println("Please enter integer");
				sc.next();
				continue;
			}
			if (choice > 11 || choice < 0)
				System.out.println("Please enter choice between 0-11");
			else
				break;
		}
		int comparison = 0;
		switch (choice) {
		case 0:
			break;
		case 1:
			System.out.println("Enter id number: ");
			int id = 0;
			while (true) {
				try {
					id = sc.nextInt();
				} catch (InputMismatchException e) {
					System.out.println("Please enter integer");
					sc.next();
					continue;
				}
				break;
			}
			comparison = getCompareChoice();
			if (comparison == 0)
				break;
			getRecordsById(id, comparison);
			break;
		case 2:
			getStringRecords(companyIndexMap);
			break;
		case 3:
			getStringRecords(drugIdIndexMap);
			break;
		case 4:
			getShortRecords(trialsIndexMap);
			break;
		case 5:
			getShortRecords(patientsIndexMap);
			break;
		case 6:
			getShortRecords(dosageIndexMap);
			break;
		case 7:
			System.out.println("Enter reading value: ");
			Float readingValue = 0.0f;

			while (true) {
				try {
					readingValue = sc.nextFloat();
				} catch (InputMismatchException e) {
					System.out.println("Please enter float value");
					sc.next();
					continue;
				}
				break;
			}
			comparison = getCompareChoice();
			if (comparison == 0)
				break;
			getRecordsByReading(readingValue, comparison);
			break;
		case 8:
			getBooleanRecords(doubleBlindIndexMap);
			break;
		case 9:
			getBooleanRecords(controlledStudyIndexMap);
			break;
		case 10:
			getBooleanRecords(govtFundedIndexMap);
			break;
		case 11:
			getBooleanRecords(fdaApprovedIndexMap);
			break;
		}
	}
   
   private static void deleteStringRecords(TreeMap<String, TreeSet<Integer>> indexMap)
   {
      Scanner sc = new Scanner(System.in);
		System.out.println("Enter value: ");
		String stringValue = sc.nextLine();
//		int comparison = getCompareChoice();
		int comparison = 1;

		if (comparison == 0)
			return;

		switch (comparison) {
		case 1:
			TreeSet<Integer> treeSet = new TreeSet<Integer>();
			treeSet = indexMap.get(stringValue.toLowerCase());
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				deleteSingleRecord(baseAddress);
			break;
		}
   }

	private static void getStringRecords(TreeMap<String, TreeSet<Integer>> indexMap) {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter value: ");
		String stringValue = sc.nextLine();
//		int comparison = getCompareChoice();
		int comparison = 1;

		if (comparison == 0)
			return;

		switch (comparison) {
		case 1:
			TreeSet<Integer> treeSet = new TreeSet<Integer>();
			treeSet = indexMap.get(stringValue.toLowerCase());
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				readSingleRecord(baseAddress);
			break;
		}

	}
   
   private static void deleteBooleanRecords(TreeMap<Boolean, TreeSet<Integer>> indexMap) {
		System.out.println("Enter boolean value(true/false): ");
		boolean booleanValue = false;
		Scanner sc = new Scanner(System.in);
		while (true) {
			try {
				booleanValue = sc.nextBoolean();
			} catch (InputMismatchException e) {
				System.out.println("Please enter boolean value");
				sc.next();
				continue;
			}
			break;
		}
		TreeSet<Integer> treeSet;
		treeSet = indexMap.get(booleanValue);
		if (treeSet == null) {
			System.out.println("This Value Doesn't Exist");
			return;
		}
		for (Integer baseAddress : treeSet)
			deleteSingleRecord(baseAddress);

	}
 
	private static void getBooleanRecords(TreeMap<Boolean, TreeSet<Integer>> indexMap) {
		System.out.println("Enter boolean value(true/false): ");
		boolean booleanValue = false;
		Scanner sc = new Scanner(System.in);
		while (true) {
			try {
				booleanValue = sc.nextBoolean();
			} catch (InputMismatchException e) {
				System.out.println("Please enter boolean value");
				sc.next();
				continue;
			}
			break;
		}
		TreeSet<Integer> treeSet;
		treeSet = indexMap.get(booleanValue);
		if (treeSet == null) {
			System.out.println("This Value Doesn't Exist");
			return;
		}
		for (Integer baseAddress : treeSet)
			readSingleRecord(baseAddress);

	}
   
   private static void deleteRecordsByReading(Float readingValue, int comparison)
   {
      TreeMap<Float, TreeSet<Integer>> indexMap = readingIndexMap;
		SortedMap<Float, TreeSet<Integer>> SubIndexMap = new TreeMap<Float, TreeSet<Integer>>();
		TreeSet<Integer> treeSet;
		switch (comparison) {
		case 1:
			treeSet = indexMap.get(readingValue);
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				deleteSingleRecord(baseAddress);
			break;

		case 2:// greater than than
			SubIndexMap = indexMap.tailMap(readingValue, false);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 3: // less than
			SubIndexMap = indexMap.headMap(readingValue, false);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 4:// greater than than equal
			SubIndexMap = indexMap.tailMap(readingValue, true);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 5: // less than equal
			SubIndexMap = indexMap.headMap(readingValue, true);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 6:
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				if (entry.getKey() == readingValue)
					continue;
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		}
   }
	private static void getRecordsByReading(Float readingValue, int comparison) {
		TreeMap<Float, TreeSet<Integer>> indexMap = readingIndexMap;
		SortedMap<Float, TreeSet<Integer>> SubIndexMap = new TreeMap<Float, TreeSet<Integer>>();
		TreeSet<Integer> treeSet;
		switch (comparison) {
		case 1:
			treeSet = indexMap.get(readingValue);
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				readSingleRecord(baseAddress);
			break;

		case 2:// greater than than
			SubIndexMap = indexMap.tailMap(readingValue, false);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 3: // less than
			SubIndexMap = indexMap.headMap(readingValue, false);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 4:// greater than than equal
			SubIndexMap = indexMap.tailMap(readingValue, true);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 5: // less than equal
			SubIndexMap = indexMap.headMap(readingValue, true);
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 6:
			for (Map.Entry<Float, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				if (entry.getKey() == readingValue)
					continue;
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		}
	}

	private static void getRecordsByCompany(String companyName, int comparison) {
		switch (comparison) {
		case 1:
			TreeSet<Integer> treeSet = new TreeSet<Integer>();
			treeSet = companyIndexMap.get(companyName);
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				readSingleRecord(baseAddress);
			break;
		}
	}
   private static void deleteRecordById(int search_id, int comparison)
   {
      SortedMap<Integer, Integer> idSubIndexMap = new TreeMap<Integer, Integer>();
      switch (comparison)
      {
         case 1:
            if (idIndexMap.containsKey(search_id))
            {
            deleteSingleRecord(idIndexMap.get(search_id));
            }
            else
				System.out.println("Matchin Record Not Found");
			break;
         case 2:// greater than than
			idSubIndexMap = idIndexMap.tailMap(search_id, false);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				deleteSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 3: // less than
			idSubIndexMap = idIndexMap.headMap(search_id, false);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				deleteSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 4:// greater than than equal
			idSubIndexMap = idIndexMap.tailMap(search_id, true);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				deleteSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 5: // less than equal
			idSubIndexMap = idIndexMap.headMap(search_id, true);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				deleteSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 6:
			for (Map.Entry<Integer, Integer> entry : idIndexMap.entrySet())
				if (entry.getKey() == search_id)
					continue;
				else
					deleteSingleRecord(idIndexMap.get(entry.getKey()));
			break;

      }
   }
   
	private static void getRecordsById(int search_id, int comparison) {
		SortedMap<Integer, Integer> idSubIndexMap = new TreeMap<Integer, Integer>();
		switch (comparison) {
		case 1:
			if (idIndexMap.containsKey(search_id))
				readSingleRecord(idIndexMap.get(search_id));
			else
				System.out.println("Matchin Record Not Found");
			break;
		case 2:// greater than than
			idSubIndexMap = idIndexMap.tailMap(search_id, false);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				readSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 3: // less than
			idSubIndexMap = idIndexMap.headMap(search_id, false);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				readSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 4:// greater than than equal
			idSubIndexMap = idIndexMap.tailMap(search_id, true);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				readSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 5: // less than equal
			idSubIndexMap = idIndexMap.headMap(search_id, true);
			for (Map.Entry<Integer, Integer> entry : idSubIndexMap.entrySet())
				readSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		case 6:
			for (Map.Entry<Integer, Integer> entry : idIndexMap.entrySet())
				if (entry.getKey() == search_id)
					continue;
				else
					readSingleRecord(idIndexMap.get(entry.getKey()));
			break;
		}
	}
   
   private static void deleteShortRecords(TreeMap<Short, TreeSet<Integer>> indexMap) {
		System.out.println("Enter value: ");
		Short shortValue = 0;
		Scanner sc = new Scanner(System.in);

		while (true) {
			try {
				shortValue = sc.nextShort();
			} catch (InputMismatchException e) {
				System.out.println("Please enter short value");
				sc.next();
				continue;
			}
			break;
		}
		int comparison = getCompareChoice();
		if (comparison == 0)
			return;
		SortedMap<Short, TreeSet<Integer>> SubIndexMap = new TreeMap<Short, TreeSet<Integer>>();
		TreeSet<Integer> treeSet;
		switch (comparison) {
		case 1:
			treeSet = indexMap.get(shortValue);
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				deleteSingleRecord(baseAddress);
			break;
		case 2:// greater than than
			SubIndexMap = indexMap.tailMap(shortValue, false);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 3: // less than
			SubIndexMap = indexMap.headMap(shortValue, false);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 4:// greater than than equal
			SubIndexMap = indexMap.tailMap(shortValue, true);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 5: // less than equal
			SubIndexMap = indexMap.headMap(shortValue, true);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		case 6:
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				if (entry.getKey() == shortValue)
					continue;
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					deleteSingleRecord(baseAddress);
			}
			break;
		}
	}
   
	private static void getShortRecords(TreeMap<Short, TreeSet<Integer>> indexMap) {
		System.out.println("Enter value: ");
		Short shortValue = 0;
		Scanner sc = new Scanner(System.in);

		while (true) {
			try {
				shortValue = sc.nextShort();
			} catch (InputMismatchException e) {
				System.out.println("Please enter short value");
				sc.next();
				continue;
			}
			break;
		}
		int comparison = getCompareChoice();
		if (comparison == 0)
			return;
		SortedMap<Short, TreeSet<Integer>> SubIndexMap = new TreeMap<Short, TreeSet<Integer>>();
		TreeSet<Integer> treeSet;
		switch (comparison) {
		case 1:
			treeSet = indexMap.get(shortValue);
			if (treeSet == null) {
				System.out.println("This Value Doesn't Exist");
				return;
			}
			for (Integer baseAddress : treeSet)
				readSingleRecord(baseAddress);
			break;
		case 2:// greater than than
			SubIndexMap = indexMap.tailMap(shortValue, false);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 3: // less than
			SubIndexMap = indexMap.headMap(shortValue, false);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 4:// greater than than equal
			SubIndexMap = indexMap.tailMap(shortValue, true);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 5: // less than equal
			SubIndexMap = indexMap.headMap(shortValue, true);
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		case 6:
			for (Map.Entry<Short, TreeSet<Integer>> entry : SubIndexMap.entrySet()) {
				if (entry.getKey() == shortValue)
					continue;
				treeSet = new TreeSet<Integer>();
				treeSet = entry.getValue();
				for (Integer baseAddress : treeSet)
					readSingleRecord(baseAddress);
			}
			break;
		}
	}
   
   public static void deleteSingleRecord(int baseAdress)
   {
       //updateBooleanIndexMaps(false,existsIndexMap,baseAdress);
      
			TreeSet<Integer> set = new TreeSet<Integer>();
			set.add(baseAdress);
			existsIndexMap.put(false, set);
		   updateBooleanIndexFiles(exists_INDEX_FILE,existsIndexMap);

       System.out.println("Succesfully deleted base address=" +baseAdress);
   }
   
	public static Record readSingleRecord(int baseAddress) {
		Record record = new Record();
      System.out.println(baseAddress);
		try {
			RandomAccessFile file = new RandomAccessFile(DB_FILE, "r");
			file.seek(baseAddress);
         System.out.println("Entered readSingleRecord");
			int id = file.readInt();
         System.out.println("Entered readSingleRecord" + id);
			byte strLen = file.readByte();
			byte byteArray[] = new byte[strLen];
			file.readFully(byteArray);
			String company = new String(byteArray);
         System.out.println("Entered readSingleRecord" + company);
			byteArray = new byte[6];
			file.readFully(byteArray);

			char[] drugid = new char[6];
			int j = 0;
			for (byte b : byteArray) {
				drugid[j] = (char) b;
				j++;
			}
			short trials = file.readShort();
         System.out.println("Entered readSingleRecord" + trials);
			short patients = file.readShort();
         System.out.println("Entered readSingleRecord" + patients);
			short dosage_mg = file.readShort();
         System.out.println("Entered readSingleRecord" + dosage_mg);
			float reading = file.readFloat();
         System.out.println("Entered readSingleRecord" + reading);
			byte flags = file.readByte();
			boolean double_blind, controlled_study, govt_funded, fda_approved;
         //exists =true;
			double_blind = ((flags & 8) == 0) ? false : true;
			controlled_study = ((flags & 4) == 0) ? false : true;
			govt_funded = ((flags & 2) == 0) ? false : true;
			fda_approved = ((flags & 1) == 0) ? false : true;
         System.out.println(file.getFilePointer()+" "+file.length());
         //if(file.getFilePointer()==file.length())
         // file.seek(file.getFilePointer()-1);
         boolean exists=file.readBoolean();
         System.out.println(file.getFilePointer());
         System.out.println(file.getFilePointer()+" "+exists);
			record = new Record(id, company, drugid, trials, patients, dosage_mg, reading,double_blind, controlled_study, govt_funded, fda_approved, exists);
			record.display();
			file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return record;
	}

	private static int getCompareChoice() {
		System.out.println("\nChoose comparison operation");
		System.out.println("1. ==");
		System.out.println("2. >");
		System.out.println("3. <");
		System.out.println("4. >=");
		System.out.println("5. <=");
		System.out.println("6. !=");
		System.out.println("0. Abort Operation");
		Scanner sc = new Scanner(System.in);
		int choice = 0;
		while (true) {
			try {
				choice = sc.nextInt();
			} catch (InputMismatchException e) {
				System.out.println("Please enter integer");
				sc.next();
				continue;
			}
			if (choice > 6 || choice < 0)
				System.out.println("Please enter choice between 0-6");
			else
				break;
		}
		return choice;
	}

	private static boolean dbExist() {
		if (DB_FILE.exists())
			return true;
		return false;
	}

	private static int printMenuOptions() {
		Scanner sc = new Scanner(System.in);

		System.out.println("\n\n-------MENU OPTIONS-------");
		System.out.println("1. Parse Input File");
		System.out.println("2. Query a field");
		System.out.println("3. Insert");
      System.out.println("4. Delete");
      System.out.println("5. Exit");
		System.out.println("Enter your choice.......");
		int choice = 0;

		while (true) {
			try {
				choice = sc.nextInt();
			} catch (InputMismatchException e) {
				System.out.println("Please enter integer");
				sc.next();
				continue;
			}
			if (choice > 5 || choice < 1)
				System.out.println("Please enter choice between 1-4");
			else
				break;
		}
		return choice;
	}

	private static void loadIndexFiles() {
		int i;
		try {
			BufferedReader br = new BufferedReader(new FileReader(ID_INDEX_FILE));
			String line;
			while ((line = br.readLine()) != null) {
				String[] stringArray = line.split(" ");
				idIndexMap.put(Integer.parseInt(stringArray[0]), Integer.parseInt(stringArray[1]));
			}
			br.close();
			br = new BufferedReader(new FileReader(COMPANY_INDEX_FILE));
			while ((line = br.readLine()) != null) {
				String[] stringArray = line.split("\t");
				TreeSet<Integer> treeSet = new TreeSet<Integer>();

				for (i = 1; i < stringArray.length; i++) {
					treeSet.add(Integer.parseInt(stringArray[i]));
				}
				companyIndexMap.put(stringArray[0].toLowerCase(), treeSet);
			}
			br.close();
			loadShortIndexMaps(TRIALS_INDEX_FILE, trialsIndexMap);
			loadShortIndexMaps(PATIENTS_INDEX_FILE, patientsIndexMap);
			loadShortIndexMaps(DOSAGE_INDEX_FILE, dosageIndexMap);
			br = new BufferedReader(new FileReader(READING_INDEX_FILE));
			while ((line = br.readLine()) != null) {
				String[] stringArray = line.split(" ");
				TreeSet<Integer> treeSet = new TreeSet<Integer>();
				for (i = 1; i < stringArray.length; i++) {
					treeSet.add(Integer.parseInt(stringArray[i]));
				}
				readingIndexMap.put(Float.parseFloat(stringArray[0]), treeSet);
			}
			br.close();
			loadBooleanIndexMaps(DOUBLEBLIND_INDEX_FILE, doubleBlindIndexMap);
			loadBooleanIndexMaps(CONTROLLED_STUDY_INDEX_FILE, controlledStudyIndexMap);
			loadBooleanIndexMaps(GOVT_FUNDED_INDEX_FILE, govtFundedIndexMap);
			loadBooleanIndexMaps(FDA_APPROVED_INDEX_FILE, fdaApprovedIndexMap);
         loadBooleanIndexMaps(exists_INDEX_FILE,existsIndexMap);
         
			br = new BufferedReader(new FileReader(DRUG_ID_INDEX_FILE));
			while ((line = br.readLine()) != null) {
				String[] stringArray = line.split("\t");
				TreeSet<Integer> treeSet = new TreeSet<Integer>();

				for (i = 1; i < stringArray.length; i++) {
					treeSet.add(Integer.parseInt(stringArray[i]));
				}
				drugIdIndexMap.put(stringArray[0].toLowerCase(), treeSet);
			}
			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void loadBooleanIndexMaps(File indexFile,TreeMap<Boolean, TreeSet<Integer>> indexMap) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(indexFile));
		int i;
		String line;
		while ((line = br.readLine()) != null) {
			String[] stringArray = line.split(" ");
			TreeSet<Integer> treeSet = new TreeSet<Integer>();
			for (i = 1; i < stringArray.length; i++) {
				treeSet.add(Integer.parseInt(stringArray[i]));
			}
			indexMap.put(Boolean.parseBoolean(stringArray[0]), treeSet);
		}
		br.close();
	}

	private static void loadShortIndexMaps(File indexFile, TreeMap<Short, TreeSet<Integer>> indexMap)
			throws NumberFormatException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(indexFile));
		int i;
		String line;
		while ((line = br.readLine()) != null) {
			String[] stringArray = line.split(" ");
			TreeSet<Integer> treeSet = new TreeSet<Integer>();
			for (i = 1; i < stringArray.length; i++) {
				treeSet.add(Integer.parseInt(stringArray[i]));
			}
			indexMap.put(Short.parseShort(stringArray[0]), treeSet);
		}
		br.close();
	}
}