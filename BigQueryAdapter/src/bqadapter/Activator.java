package bqadapter;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sap.hana.dp.adapter.sdk.AdapterFactory;

public class Activator implements BundleActivator {	
	
	//System.setProperty("java.util.logging.config.file", fileName);
	Logger logger = LogManager.getLogger(Activator.class);

	private static BundleContext context;

	static BundleContext getContext() {
		return context;
	}

	ServiceRegistration<?> adapterRegistration;

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	
	@Override
	public void start(BundleContext bundleContext) throws Exception {		
		
		PropertyConfigurator.configure("log4j.properties");
        logger.info("### BQAdapter start");
        
		Activator.context = bundleContext;
	
		BQAdapterFactory srv = new BQAdapterFactory();
		adapterRegistration = context.registerService(AdapterFactory.class.getName(),srv ,null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext bundleContext) throws Exception {
		logger.info("### BQAdapter stop");
		Activator.context = null;

		adapterRegistration.unregister();
	}

}
