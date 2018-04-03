/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.test;

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator;
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase;


/**
 *
 * @author nicholaskoutroumanis
 */
public class Test {

    public static void main(String args[]) {

//        MyOpVisitorBase.newMyOpVisitorBase(
//"SELECT ?name " +
//"WHERE" +
//"{" +
//" ?x <1> ?k ." +
//" ?y <gfg> 'b'." + 
//" ?x <re> 'sd'" + 
//"}").getBop(); 
        
       BaseOperator[] bop = MyOpVisitorBase.newMyOpVisitorBase("SELECT ?x" +
"WHERE" +
"{" +
" ?x <1> 'a' ." +
" ?x <2> ?k ." +
" 'e' <3> ?y ." +
" ?x <4> 'd' ." +  
" ?y <1> 'sd' ." +
" ?y <2> ?q ." +
" 'e' <876> ?y ." +
" ?x <sdfg> 'dd' ." + 
               
" ?x <1> ?x ." +
" ?f <2> ?k ." +
" 'f' <3> ?y ." +
" ?g <4> 'd' ." +  
" ?y <1> ?g ." +
" ?y <2> ?x ." +
" 'e' <fd> ?y ." +
" ?c <sdfg> ?x ." +                  
               
               
               
"}").getBop();
       
        System.out.println("NumberOfTrees: " + bop.length);
        for(BaseOperator b:bop){
            System.out.println("--------------------------");
            System.out.println(bop[0].toString());
        }
    }
}
