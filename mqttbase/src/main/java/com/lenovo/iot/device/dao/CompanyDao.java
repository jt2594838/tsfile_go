package com.lenovo.iot.device.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.lenovo.iot.device.model.Company;
import com.lenovo.iot.device.model.Device;

@Repository
public class CompanyDao {

    @Autowired
    private JdbcTemplate jdbcTemplateMysql;

    public Company getCompany(long company_id) {
        Company company = null;

        try {
            String sql = "select company_id, company_name, company_sk, private_key, public_key, create_stamp, update_stamp from company where company_id = ?";

            List<Company> list = jdbcTemplateMysql.query(sql, new Object[]{company_id}, Company.MAP);
            if (list != null && list.size() > 0) {
                return (Company) list.get(0);
            }
        } catch (EmptyResultDataAccessException e) {
            company = null;
        }

        return company;
    }

    public String getSecretKey(long company_id) {
        String sk;

        try {
            String sql = "select company_sk from company where company_id = ?";
            sk = jdbcTemplateMysql.queryForObject(sql, new Object[]{company_id}, String.class);
        } catch (EmptyResultDataAccessException e) {
            sk = null;
        }

        return sk;
    }

//    public int updateCompany(long company_id, String company_name, String company_sk) {
//        String sql = "update company set company_name = ?, company_sk = ? where company_id = ?";
//        int row = jdbcTemplateMysql.update(sql, new Object[]{company_name, company_sk, company_id});
//        return row;
//    }
//
//    public int saveDeviceSk(long company_id, String device_sk) {
//        String sql = "update company set device_sk = ? where company_id = ?";
//        int row = jdbcTemplateMysql.update(sql, new Object[]{device_sk, company_id});
//        return row;
//    }
//
//    public String getDeviceSk(long company_id) {
//        String sql = "select device_sk from company where company_id = ?";
//        String deviceSkStr = jdbcTemplateMysql.queryForObject(sql, new Object[]{company_id}, String.class);
//        return deviceSkStr;
//    }
}
