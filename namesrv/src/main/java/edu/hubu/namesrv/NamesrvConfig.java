package edu.hubu.namesrv;

/**
 * @author: sugar
 * @date: 2023/6/10
 * @description:
 */
public class NamesrvConfig {
    private boolean orderMessageEnable = false;

    public boolean isOrderMessageEnable() {
        return orderMessageEnable;
    }

    public void setOrderMessageEnable(boolean orderMessageEnable) {
        this.orderMessageEnable = orderMessageEnable;
    }
}
