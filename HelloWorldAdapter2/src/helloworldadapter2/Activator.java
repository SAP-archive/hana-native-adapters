package helloworldadapter2;

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
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	public void start(BundleContext bundleContext) throws Exception {
		Activator.context = bundleContext;
		System.out.println("Adapter "+ "HelloWorldAdapter2" +" Started");
		HelloWorldAdapter2Factory srv = new HelloWorldAdapter2Factory();
		adapterRegistration = context.registerService(AdapterFactory.class.getName(),srv ,null);

	}

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext bundleContext) throws Exception {
		Activator.context = null;
		System.out.println("Adapter "+ "HelloWorldAdapter2" +" stopped");	
		adapterRegistration.unregister();

	}

}