package io.sustc.service.impl;

import io.sustc.DatabaseConnection.SQLDataSource;
import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.pojo.OAMessage;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.asm.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.net.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static io.sustc.service.ValidationCheck.UserValidationCheck.HasResultAndSet;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    private SQLDataSource dataSource;

    public VideoServiceImpl(){
        dataSource = new SQLDataSource(24);
    }
    public String postVideo(AuthInfo auth, PostVideoReq req){
        if(req.getTitle().isEmpty() || req.getTitle()==null){
            log.info("No title");
            return null;}

        if(req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())){
            log.info("before we start");return null;
        }
        if(req.getDuration() < 10){
            log.info("短视频！b站不会变味！");return null;
        }

        //check OA
        OAMessage oaMessage = checkAuthInvalid(auth);
        if(!oaMessage.isAuthIsValid()){
            log.info("OA failed");
            return null;}

        String CHA_CHONG ="SELECT title from b_video where title =? and owner_mid =?";
        try (Connection con = dataSource.getSQLConnection();
             PreparedStatement preparedStatement = con.prepareStatement(CHA_CHONG)){
            preparedStatement.setString(1,req.getTitle());
            preparedStatement.setLong(2,oaMessage.getMid());
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){log.info("已经存在视频了！");return null;}

            String NewBV = "SELECT LEFT(regexp_replace(translate(uuid_generate_v4()::text, '-', '')," +
                    " '[^a-zA-Z0-9]', '', 'g'), 9) AS custom_length_uuid";

            String bv = "BV1";

            PreparedStatement stmt = con.prepareStatement(NewBV);
            ResultSet resultSet1 = stmt.executeQuery();
            if(resultSet1.next()){
                bv += resultSet1.getString(1);
            }
            stmt.close();
            log.info(bv);

            log.info("start to insert video");
            String InsertVideo =
                    "INSERT INTO b_video" +
                            "(bv, title, owner_mid, owner_name, " +
                            "commit_time, review_time, public_time, duration," +
                            " description, reviewer, update_time,create_time,is_public,is_posted,is_review) " +
                            "values " +
                            "(?,?,?,?" +
                            ",CURRENT_TIMESTAMP,null,null,?" +
                            ",?,null,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,0,1,0)";

            PreparedStatement stmt2 = con.prepareStatement(InsertVideo);
            stmt2.setString(1,bv);
            stmt2.setString(2, req.getTitle());
            stmt2.setLong(3,oaMessage.getMid());
            stmt2.setString(4,oaMessage.getName());

            stmt2.setFloat(5,req.getDuration());
            if(req.getDescription().isEmpty()||req.getDescription()==null){
                stmt2.setNull(6, Type.CHAR);
            }else {stmt2.setString(6,req.getDescription());}

            log.info("Get Ready");
            stmt2.executeUpdate();


            stmt2.close();
            con.close();


        }catch (SQLException e){
            log.info("SQL?");
            return null;

        }
        return null;
    }
    public boolean deleteVideo(AuthInfo auth, String bv){return false;}
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req){return false;}
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum){return null;}
    public double getAverageViewRate(String bv){return 0;}
    public Set<Integer> getHotspot(String bv){return null;}
    public  boolean reviewVideo(AuthInfo auth, String bv){return false;}
    public boolean coinVideo(AuthInfo auth, String bv){return false;}
    public boolean likeVideo(AuthInfo auth, String bv){return false;}
    public boolean collectVideo(AuthInfo auth, String bv){return false;}


    public OAMessage checkAuthInvalid(AuthInfo auth){


        OAMessage message = new OAMessage();
        if(auth.getMid()<=0){//don't have mid
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //2A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth2A = "SELECT * from b_user where qq = ? and wechat= ?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);
                        stmt.setString(1,auth.getQq());
                        stmt.setString(2,auth.getWechat());

                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person

                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//qq 1A
                    try(Connection con = dataSource.getSQLConnection()){
//                        Connection con = dataSource.getSQLConnection();
                        String AuthA = "SELECT * from b_user where qq = ? ";
                        PreparedStatement stmt = con.prepareStatement(AuthA);

                        stmt.setString(1,auth.getQq());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String AuthA = "SELECT * from b_user where wechat = ?  ";
                        PreparedStatement stmt = con.prepareStatement(AuthA); //1A
                        stmt.setString(1, auth.getWechat());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){  //0A
                    // nmdx,shen me dou mei you ni deng ge ji er
                    return message;
                }
            }
            else {//has password
                if(auth.getQq()!=null && auth.getWechat()!=null){ //3A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth3A = "SELECT * from b_user " +
                                "where wechat = ? and qq = ? and password = ? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getQq());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){  //2A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        Connection con = dataSource.getSQLConnection();
                        String Auth2A = "SELECT * from b_user " +
                                "where wechat = ? and password =?  ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setString(1,auth.getWechat());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where qq = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setString(1,auth.getQq());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//A
                    return message;//only password
                }
            }

        }
        else {//HAVE MID
            if(auth.getPassword()==null){//don't have password
                if(auth.getQq()!=null && auth.getWechat()!=null){//3A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and wechat = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());
                        stmt.setString(2,auth.getWechat());
                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and wechat =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//2A
                    try(Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and qq =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getQq());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//A
                    return message;
                }
            }else {// have password
                if(auth.getQq()!=null && auth.getWechat()!=null){//4A
                    try(Connection con = dataSource.getSQLConnection()) {
//                        System.out.println("4A");


//                        System.out.println(dataSource.toStrin
                        String Auth4A = "SELECT * from b_user " +
                                "where mid = ? and wechat = ? and qq = ? and password = ? ";

                        PreparedStatement stmt = con.prepareStatement(Auth4A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());

                        stmt.setString(3,auth.getQq());

                        stmt.setString(4,auth.getPassword());

                        ResultSet resultSet = stmt.executeQuery();
//                        if(!resultSet.next()){System.out.println("hey");}
//                        System.out.println(1);
//                        System.out.println(resultSet.toString());
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()!=null){//3A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and wechat =? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getWechat());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()!=null && auth.getWechat()==null){//3A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth3A = "SELECT * from b_user " +
                                "where mid = ? and qq =? and password=? ";
                        PreparedStatement stmt = con.prepareStatement(Auth3A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getQq());

                        stmt.setString(3,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
                if(auth.getQq()==null && auth.getWechat()==null){//2A
                    try (Connection con = dataSource.getSQLConnection()) {

                        String Auth2A = "SELECT * from b_user " +
                                "where mid = ? and password =? ";
                        PreparedStatement stmt = con.prepareStatement(Auth2A);

                        stmt.setLong(1,auth.getMid());

                        stmt.setString(2,auth.getPassword());
                        ResultSet resultSet = stmt.executeQuery();
                        HasResultAndSet(message, resultSet);
                        return message;// don't have the person
                    } catch (SQLException e) {
                        return message;
                    }
                }
            }
        }
        return message;
    }

}
