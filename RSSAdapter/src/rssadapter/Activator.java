package rssadapter;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import com.sap.hana.dp.adapter.sdk.AdapterFactory;

public class Activator  implements BundleActivator {

	private static BundleContext context;

	static BundleContext getContext() {
		return context;
	}

	ServiceRegistration<?> adapterRegistration;
//	ServiceRegistration<?> commandRegistration;
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	public void start(BundleContext bundleContext) throws Exception {
		Activator.context = bundleContext;
		System.out.println("Adapter "+ "RSSAdapter" +" Started");
		RSSAdapterFactory srv = new RSSAdapterFactory();
		adapterRegistration = context.registerService(AdapterFactory.class.getName(),srv ,null);
/*		Dictionary<String, Object> properties = new Hashtable<String, Object>();
		properties.put("osgi.command.scope", "Data Provisioning");
		properties.put("osgi.command.function", new String[] {"dpregister"});
		commandRegistration = context.registerService(DPCommandProvider.class.getName(), new DPCommandProvider(), properties); 
		*/
	}

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext bundleContext) throws Exception {
		Activator.context = null;
		System.out.println("Adapter "+ "RSSAdapter" +" stopped");	
		adapterRegistration.unregister();
		// commandRegistration.unregister();
	}

}