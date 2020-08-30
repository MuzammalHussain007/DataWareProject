import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DataWareProject {
    private String[] tempArr= new String[100000];
    private List<String> state= new ArrayList<>();
    private List<String> totalprimary= new ArrayList<>();
    private List<String> year,uperprimary,secondry,highersecondry,allschool;
    private String filePath="C:\\Users\\Adminstrator\\Desktop\\DataWareHouseProject\\data\\schools-with-girls-toilet-2013-2016.csv";

    private Connection con;
    private Statement statement;
    private PreparedStatement prep;
    private ResultSet rs;
    private String query;

    public static final String delimiter = ",";

    public DataWareProject() throws ClassNotFoundException, SQLException {

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        con= DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=DatawareProject;integratedSecurity=true");
        statement=con.createStatement();
        rs=null;
        prep=null;
        query=new String();
        year= new ArrayList<>();
        uperprimary= new ArrayList<>();
        secondry= new ArrayList<>();
        highersecondry= new ArrayList<>();
        allschool= new ArrayList<>();
   //    read(filePath);
      //  seperateState(filePath);
       seperateTotalPrimary(filePath);
        seperateYear(filePath);
       seperateUperprimary(filePath);
         seperateSecondry(filePath);
       seperateHigherSecondry(filePath);
        seperateAllSchool(filePath);



    //   show();

    }

    private void show() {
        for (int i=0;i<state.size();i++)
        {

            System.out.println(state.get(i));
        }
    }

    public void read(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                System.out.println(tempArr[2]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateState(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
             //   System.out.println(tempArr[0]);
                state.add(tempArr[0]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateTotalPrimary(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                  System.out.println(tempArr[2]);
                totalprimary.add(tempArr[2]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateYear(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                  System.out.println(tempArr[1]);
                year.add(tempArr[1]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateUperprimary(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                 System.out.println(tempArr[5]);
                uperprimary.add(tempArr[5]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateSecondry(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                   System.out.println(tempArr[9]);
                secondry.add(tempArr[11]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateHigherSecondry(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                   System.out.println(tempArr[11]);

                highersecondry.add(tempArr[11]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void seperateAllSchool(String csvFile) {
        try {
            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line=br.readLine();
            while ((line = br.readLine()) != null) {
                tempArr = line.split(delimiter);
                System.out.println(tempArr[12]);

                allschool.add(tempArr[12]);
                System.out.println();
            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    public void  insetState() throws SQLException {
        for (int i=0;i<state.size();i++)
        {
            query=" insert into States values('"+state.get(i)+"')";
            statement.executeUpdate(query);
        }

    }
    public void insertGrossenrollment() throws SQLException {
         int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
           query="INSERT INTO GrossEnrollment  values(?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }
    public void insertSchoolwithcampus() throws SQLException {
        int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
            query="INSERT INTO School_with_campus  values(?,?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6,allschool.get(i));
            prep.setString(7, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }
    public void insertSchoolwithelectricity() throws SQLException {
        int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
            query="INSERT INTO School_with_electricity  values(?,?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6,allschool.get(i));
            prep.setString(7, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }
    public void insertSchoolwithwaterfacility() throws SQLException {
        int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
            query="INSERT INTO School_with_water values(?,?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6,allschool.get(i));
            prep.setString(7, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }
    public void insertSchoolwithboystoilet() throws SQLException {
        int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
            query="INSERT INTO School_with_Boyes_toilet values(?,?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6,allschool.get(i));
            prep.setString(7, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }
    public void insertSchoolwithgirlsstoilet() throws SQLException {
        int stateid=0;
        for (int i=0;i<year.size();i++)
        {
            stateid++;
            System.out.println(year.get(i));
            query="INSERT INTO School_with_Girls_toilet values(?,?,?,?,?,?,?)";
            prep=con.prepareStatement(query);
            prep.setString(1,year.get(i));
            prep.setString(2,totalprimary.get(i));
            prep.setString(3,uperprimary.get(i));
            prep.setString(4,secondry.get(i));
            prep.setString(5,highersecondry.get(i));
            prep.setString(6,allschool.get(i));
            prep.setString(7, String.valueOf(stateid));

            int result=prep.executeUpdate();
            System.out.println(result);

        }
    }











}


