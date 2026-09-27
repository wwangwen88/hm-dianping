package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 创建订单（异步执行，所以没有返回值）
     * 注意：必须声明在接口中，否则 JDK 动态代理拦不到它，@Transactional 会失效
     */
    void createVoucherOrder(VoucherOrder voucherOrder);
}
