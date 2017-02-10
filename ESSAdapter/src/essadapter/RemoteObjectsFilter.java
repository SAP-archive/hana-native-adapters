package essadapter;


/**
 * The Class RemoteObjectsFilter.
 */
public class RemoteObjectsFilter {

    /**  remote objects filter applied on database name. */
    private String filterDatabaseName;

    /**  remote objects filter applied on owner name. */
    private String filterOwnerName;

    /**  remote objects filter applied on physical name. */
    private String filterPhysicalName;

    /**  remote objects filter applied on unique name. */
    private String filterUniqueName;

    /**
     * Default constructor.
     */
    public RemoteObjectsFilter() {
        
    }

    /**
     * RemoteObjectsFilter constructor with default filters applied on
     * database_name/owner_name/physical_name/unique_name.
     *
     * @param filterDatabaseName
     *            default filter applied on database name
     * @param filterOwnerName
     *            default filter applied on owner name
     * @param filterPhysicalName
     *            default filter applied on physical name
     * @param filterUniqueName
     *            default filter applied on unique name
     */
    public RemoteObjectsFilter(String filterDatabaseName, String filterOwnerName,
            String filterPhysicalName, String filterUniqueName) {
        this.filterDatabaseName = filterDatabaseName;
        this.filterOwnerName = filterOwnerName;
        this.filterPhysicalName = filterPhysicalName;
        this.filterUniqueName = filterUniqueName;
    }
    
    /**
     * Set the current filter applied on specified database name.
     *
     * @param filterDatabaseName filter applied on database name
     */
    public void setFilterDatabaseName(String filterDatabaseName) {
        this.filterDatabaseName = filterDatabaseName;
    }
    
    /**
     * Return current filter applied on specified database name as a String.
     *
     * @return filter applied on database name
     */
    public String getFilterDatabaseName() {
        return this.filterDatabaseName;
    }
    
    /**
     * Set the current filter applied on specified owner name.
     *
     * @param filterOwnerName filter applied on owner name
     */
    public void setFilterOwnerName(String filterOwnerName) {
        this.filterOwnerName = filterOwnerName;
    }
    
    /**
     * Return current filter applied on specified owner name as a String.
     *
     * @return filter applied on owner name
     */
    public String getFilterOwnerName() {
        return this.filterOwnerName;
    }
    
    /**
     * Set the current filter applied on specified physical name.
     *
     * @param filterPhysicalName filter applied on physical name
     */
    public void setFilterPhysicalName(String filterPhysicalName) {
        this.filterPhysicalName = filterPhysicalName;
    }
    
    /**
     * Return current filter applied on specified physical name as a String.
     *
     * @return filter applied on physical name
     */
    public String getFilterPhysicalName() {
        return this.filterPhysicalName;
    }

    /**
     * Set the current filter applied on specified unique name.
     *
     * @param filterUniqueName filter applied on unique name
     */
    public void setFilterUniqueName(String filterUniqueName) {
        this.filterUniqueName = filterUniqueName;
    }
    
    /**
     * Return current filter applied on specified unique name as a String.
     *
     * @return filter applied on unique name
     */
    public String getFilterUniqueName() {
        return this.filterUniqueName;
    }

}
