/*
 * Copyright 2013-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.cloud.nacos.registry;

import java.util.List;
import java.util.Properties;

import com.alibaba.cloud.commons.lang.StringUtils;
import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
import com.alibaba.cloud.nacos.NacosServiceManager;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

import static org.springframework.util.ReflectionUtils.rethrowRuntimeException;

/**
 * @author xiaojing
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @author <a href="mailto:78552423@qq.com">eshun</a>
 * @author JAY
 */
public class NacosServiceRegistry implements ServiceRegistry<Registration> {

	private static final String STATUS_UP = "UP";

	private static final String STATUS_DOWN = "DOWN";

	private static final Logger log = LoggerFactory.getLogger(NacosServiceRegistry.class);

	private final NacosDiscoveryProperties nacosDiscoveryProperties;

	private final NacosServiceManager nacosServiceManager;

	public NacosServiceRegistry(NacosServiceManager nacosServiceManager,
			NacosDiscoveryProperties nacosDiscoveryProperties) {
		this.nacosDiscoveryProperties = nacosDiscoveryProperties;
		this.nacosServiceManager = nacosServiceManager;
	}

	@Override
	public void register(Registration registration) {

		// 如果注册实例信息中的服务ID 是空的那么就不是有效的服务实例，不允许注册
		// service-provider
		if (StringUtils.isEmpty(registration.getServiceId())) {
			log.warn("No service to register for nacos client...");
			return;
		}

		// 获取名称空间
		NamingService namingService = namingService();
		// 获取注册项目的服务 ID ：service-provider
		String serviceId = registration.getServiceId();
		// 获取名称空间的组信息 DEFAULT_GROUP
		String group = nacosDiscoveryProperties.getGroup();

		// 构建实例信息
		Instance instance = getNacosInstanceFromRegistration(registration);

		try {
			// 进行服务实例注册
			namingService.registerInstance(serviceId, group, instance);
			log.info("nacos registry, {} {} {}:{} register finished", group, serviceId,
					instance.getIp(), instance.getPort());
		}
		catch (Exception e) {
			if (nacosDiscoveryProperties.isFailFast()) {
				log.error("nacos registry, {} register failed...{},", serviceId,
						registration.toString(), e);
				rethrowRuntimeException(e);
			}
			else {
				log.warn("Failfast is false. {} register failed...{},", serviceId,
						registration.toString(), e);
			}
		}
	}

	@Override
	public void deregister(Registration registration) {

		log.info("De-registering from Nacos Server now...");

		if (StringUtils.isEmpty(registration.getServiceId())) {
			log.warn("No dom to de-register for nacos client...");
			return;
		}

		// 获取命名空间
		NamingService namingService = namingService();
		// 获取注册中心服务的ID
		String serviceId = registration.getServiceId();
		// 获取组，默认就是 default
		String group = nacosDiscoveryProperties.getGroup();

		// 通过 namingService 进行客户端实例下线
		try {
			namingService.deregisterInstance(serviceId, group, registration.getHost(),
					registration.getPort(), nacosDiscoveryProperties.getClusterName());
		}
		catch (Exception e) {
			log.error("ERR_NACOS_DEREGISTER, de-register failed...{},",
					registration.toString(), e);
		}

		log.info("De-registration finished.");
	}

	@Override
	public void close() {
		try {
			// nacos 服务管理下线
			nacosServiceManager.nacosServiceShutDown();
		}
		catch (NacosException e) {
			log.error("Nacos namingService shutDown failed", e);
		}
	}

	@Override
	public void setStatus(Registration registration, String status) {

		if (!STATUS_UP.equalsIgnoreCase(status)
				&& !STATUS_DOWN.equalsIgnoreCase(status)) {
			log.warn("can't support status {},please choose UP or DOWN", status);
			return;
		}

		String serviceId = registration.getServiceId();

		Instance instance = getNacosInstanceFromRegistration(registration);

		if (STATUS_DOWN.equalsIgnoreCase(status)) {
			instance.setEnabled(false);
		}
		else {
			instance.setEnabled(true);
		}

		try {
			Properties nacosProperties = nacosDiscoveryProperties.getNacosProperties();
			nacosServiceManager.getNamingMaintainService(nacosProperties).updateInstance(
					serviceId, nacosDiscoveryProperties.getGroup(), instance);
		}
		catch (Exception e) {
			throw new RuntimeException("update nacos instance status fail", e);
		}

	}

	@Override
	public Object getStatus(Registration registration) {

		String serviceName = registration.getServiceId();
		String group = nacosDiscoveryProperties.getGroup();
		try {
			List<Instance> instances = namingService().getAllInstances(serviceName,
					group);
			for (Instance instance : instances) {
				if (instance.getIp().equalsIgnoreCase(nacosDiscoveryProperties.getIp())
						&& instance.getPort() == nacosDiscoveryProperties.getPort()) {
					return instance.isEnabled() ? STATUS_UP : STATUS_DOWN;
				}
			}
		}
		catch (Exception e) {
			log.error("get all instance of {} error,", serviceName, e);
		}
		return null;
	}

	/**
	 * 构建实例信息
	 *
	 * @param registration
	 * @return
	 */
	private Instance getNacosInstanceFromRegistration(Registration registration) {
		Instance instance = new Instance();
		// ip设置
		instance.setIp(registration.getHost());
		// 端口设置
		instance.setPort(registration.getPort());
		// 权重设置
		instance.setWeight(nacosDiscoveryProperties.getWeight());
		// 集群名称
		instance.setClusterName(nacosDiscoveryProperties.getClusterName());
		// 是否开启
		instance.setEnabled(nacosDiscoveryProperties.isInstanceEnabled());
		// 元数据
		instance.setMetadata(registration.getMetadata());
		instance.setEphemeral(nacosDiscoveryProperties.isEphemeral());
		// 返回实例
		return instance;
	}

	private NamingService namingService() {
		// 根据提供的nacos属性值进行命名空间信息获取
		return nacosServiceManager.getNamingService();
	}

}
