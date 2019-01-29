package com.thebeastshop.liteflow.core;


import com.google.common.collect.Lists;
import com.thebeastshop.liteflow.entity.config.*;
import com.thebeastshop.liteflow.entity.data.DataBus;
import com.thebeastshop.liteflow.entity.data.DefaultSlot;
import com.thebeastshop.liteflow.entity.data.Slot;
import com.thebeastshop.liteflow.exception.ChainNotFoundException;
import com.thebeastshop.liteflow.exception.FlowExecutorNotInitException;
import com.thebeastshop.liteflow.exception.FlowSystemException;
import com.thebeastshop.liteflow.exception.NoAvailableSlotException;
import com.thebeastshop.liteflow.executor.NamedThreadFactory;
import com.thebeastshop.liteflow.flow.FlowBus;
import com.thebeastshop.liteflow.parser.LocalXmlFlowParser;
import com.thebeastshop.liteflow.parser.XmlFlowParser;
import com.thebeastshop.liteflow.parser.ZookeeperXmlFlowParser;
import com.thebeastshop.liteflow.util.PatternUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class FlowExecutor {
	
	private static final Logger LOG = LoggerFactory.getLogger(FlowExecutor.class);

	private static final int DEFAULT_CORE_POOL_SIZE = 50;

	private static final int DEFAULT_MAX_POOL_SIZE = 100;

	private static final int DEFAULT_QUEUE_CAPACITY = 100;

	private static final long KEEP_ALIVE_TIME = 100L;

	private  long threadPoolTimeout = 1000;

	private ThreadPoolExecutor threadPool;


	private List<String> rulePath;
	
	private String zkNode;
	
	public void init() {

		if (threadPool==null){
			threadPool= new ThreadPoolExecutor(DEFAULT_CORE_POOL_SIZE, DEFAULT_MAX_POOL_SIZE,
					KEEP_ALIVE_TIME, TimeUnit.MINUTES,  new LinkedBlockingQueue<Runnable>(DEFAULT_QUEUE_CAPACITY), new NamedThreadFactory("fast flow"),
					new ThreadPoolExecutor.AbortPolicy() {
						@Override
						public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
							LOG.error("thread pool is full, reject request");
							throw new RejectedExecutionException("Task " + r.toString() +
									" rejected from " +
									executor.toString());
						}

					}
			);
		}

		XmlFlowParser parser = null;
		for(String path : rulePath){
			try {
				if(PatternUtil.isLocalConfig(path)) {
					parser = new LocalXmlFlowParser();
				}else if(PatternUtil.isZKConfig(path)){
					if(StringUtils.isNotBlank(zkNode)) {
						parser = new ZookeeperXmlFlowParser(zkNode);
					}else {
						parser = new ZookeeperXmlFlowParser();
					}
				}else if(PatternUtil.isClassConfig(path)) {
					Class c = Class.forName(path);
					parser = (XmlFlowParser)c.newInstance();
				}

				Objects.requireNonNull(parser).parseMain(path);
			} catch (Exception e) {
				String errorMsg = MessageFormat.format("init flow executor cause error,cannot parse rule file{0}", path);
				LOG.error(errorMsg,e);
				throw new FlowExecutorNotInitException(errorMsg);
			}
		}
	}
	

	
	public void reloadRule(){
		init();
	}

	public <T extends Slot> T execute(String chainId, Object param){
		return execute(chainId, param, DefaultSlot.class,null,false);
	}
	
	public <T extends Slot> T execute(String chainId,Object param,Class<? extends Slot> slotClazz){
		return execute(chainId, param, slotClazz,null,false);
	}
	
	public void invoke(String chainId,Object param,Class<? extends Slot> slotClazz,Integer slotIndex){
		execute(chainId, param, slotClazz,slotIndex,true);
	}
	
	public <T extends Slot> T execute(String chainId,Object param,Class<? extends Slot> slotClazz,Integer slotIndex,boolean isInnerChain){
		Slot slot = null;
		try{
			if(FlowBus.needInit()) {
				init();
			}

			if(!isInnerChain && slotIndex == null) {
				slotIndex = DataBus.offerSlot(slotClazz);
				LOG.info("slot[{}] offered",slotIndex);
			}

			if(slotIndex == -1){
				throw new NoAvailableSlotException("there is no available slot");
			}

			slot = DataBus.getSlot(slotIndex);
			if(slot == null) {
				throw new NoAvailableSlotException("the slot is not exist");
			}

			if(StringUtils.isBlank(slot.getRequestId())) {
				slot.generateRequestId();
				LOG.info("requestId[{}] has generated", slot.getRequestId());
			}

			if(!isInnerChain) {
				slot.setRequestData(param);
				slot.setChainName(chainId);
			}else {
				slot.setChainReqData(chainId, param);
			}

			Chain chain = FlowBus.getChain(chainId);

			if(chain == null|| CollectionUtils.isEmpty(chain.getConditionList())){
				String errorMsg = MessageFormat.format("couldn't find chain with the id[{0}]", chainId);
				throw new ChainNotFoundException(errorMsg);
			}

			run(chain.getConditionList(),slot,slotIndex);

			return (T)slot;
		}catch(Exception e){
			String errorMsg = MessageFormat.format("[{0}]executor cause error", slot==null?"slot is null":slot.getRequestId());
			LOG.error(errorMsg,e);
			throw new FlowSystemException(errorMsg);
		}finally{
			if(!isInnerChain) {
				if (slot != null) {
					slot.printStep();
				}
				if (slotIndex!=null){
					DataBus.releaseSlot(slotIndex);
				}
			}
		}
	}

	private void run(List<Condition> conditionList,Slot slot,Integer slotIndex) throws Exception {
		List<Node> nodeList;
		NodeComponent component;
		List<Node> executedNodeList = Lists.newArrayList();
		for(Condition condition : conditionList){
			boolean needRollBack = false;
			Exception exception = null;

			nodeList = condition.getNodeList();
			if(condition instanceof ThenCondition){
				for(Node node : nodeList){
					component = node.getInstance();
					try{
						component.setSlotIndex(slotIndex);
						if(component.isAccess()){
							executedNodeList.add(node);
							component.execute();
							if(component.isEnd()) {
								LOG.info("[{}]:component[{}] lead the chain to end",slot.getRequestId(),component.getClass().getSimpleName());
								break;
							}
						}else {
							LOG.info("[{}]:[X]skip component[{}] execution",slot.getRequestId(),component.getClass().getSimpleName());
						}
					}catch(Exception t){
						if(component.isContinueOnError()){
							String errorMsg = MessageFormat.format("[{0}]:component[{1}] cause error,but flow is still go on", slot.getRequestId(),component.getClass().getSimpleName());
							LOG.error(errorMsg,t);
						}else{
							String errorMsg = MessageFormat.format("[{0}]:executor cause error",slot.getRequestId());
							LOG.error(errorMsg,t);
							exception=t;
							needRollBack = true;
						}
					}
				}
			}else if(condition instanceof WhenCondition){
				final  List<Future> futureList = new ArrayList<>(nodeList.size());
				for(Node node : nodeList){
					executedNodeList.add(node);
					futureList.add(threadPool.submit(new WhenConditionThread(node,slotIndex,slot.getRequestId())));
				}
				for (int i=0;i<futureList.size();i++) {
					try {
						futureList.get(i).get(threadPoolTimeout, TimeUnit.MILLISECONDS);
					}  catch (Exception e) {
						LOG.error("fast flow execute error, node:{}, throw exception,", nodeList.get(i).getId(), e);
						exception=e;
						needRollBack = true;
					}
				}
			}

			if (needRollBack){
				rollbackNode(executedNodeList,slotIndex);
				throw exception;
			}
		}
	}



	private void rollbackNode(List<Node> executedNodeList,Integer slotIndex) {
		Collections.reverse(executedNodeList);
		for (Node node:executedNodeList){
			try {
				NodeComponent nodeComponent=node.getInstance();
				nodeComponent.setSlotIndex(slotIndex);
				if (nodeComponent.isAccess()){
					nodeComponent.rollbackExecute();
				}
			}catch (Exception e){
				LOG.error("fast flow rollbackNode is error,node {} ",node.getId(),e);
			}
		}
	}

	private class WhenConditionThread implements Callable<String> {

		private Node node;

		private Integer slotIndex;

		private String requestId;


		WhenConditionThread(Node node, Integer slotIndex, String requestId){
			this.node = node;
			this.slotIndex = slotIndex;
			this.requestId = requestId;

		}


		@Override
		public String call() throws Exception {
			NodeComponent cmp = node.getInstance().setSlotIndex(slotIndex);
			if (cmp.isAccess()) {
				cmp.execute();
			} else {
				LOG.info("[{}]:[X]skip component[{}] execution", requestId, cmp.getClass().getSimpleName());
			}
			return "";
		}
	}

	public List<String> getRulePath() {
		return rulePath;
	}

	public void setRulePath(List<String> rulePath) {
		this.rulePath = rulePath;
	}
	
	public String getZkNode() {
		return zkNode;
	}

	public void setZkNode(String zkNode) {
		this.zkNode = zkNode;
	}


	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

	public void setThreadPool(ThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}

	public long getThreadPoolTimeout() {
		return threadPoolTimeout;
	}

	public void setThreadPoolTimeout(long threadPoolTimeout) {
		this.threadPoolTimeout = threadPoolTimeout;
	}
}
