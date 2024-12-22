package qengine_concurrent.benchmark;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

public class MachineInfo {

    public static String getMachineInfo() {
        StringBuilder sb = new StringBuilder();
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        OperatingSystem os = si.getOperatingSystem();
        CentralProcessor processor = hal.getProcessor();
        GlobalMemory memory = hal.getMemory();

        sb.append("Operating System: ").append(os).append("\n");
        sb.append("Processor: ").append(processor.getProcessorIdentifier().getName()).append("\n");
        sb.append("Logical Cores: ").append(processor.getLogicalProcessorCount()).append("\n");
        sb.append("Physical Cores: ").append(processor.getPhysicalProcessorCount()).append("\n");
        sb.append("Total Memory: ").append(memory.getTotal() / (1024 * 1024)).append(" MB (").append(memory.getTotal() / (1024 * 1024 * 1024)).append(" GB)").append("\n");
        sb.append("Available Memory: ").append(memory.getAvailable() / (1024 * 1024)).append(" MB (").append(memory.getAvailable() / (1024 * 1024 * 1024)).append(" GB)").append("\n");
        sb.append("Processor Frequency: ").append(processor.getProcessorIdentifier().getVendorFreq() / 1_000_000).append(" MHz").append("\n");
        si.getOperatingSystem().getFileSystem().getFileStores().forEach(fs -> {
            sb.append("Disk: ").append(fs.getName())
                    .append(" Total: ").append(fs.getTotalSpace() / (1024 * 1024 * 1024)).append(" GB")
                    .append(" Free: ").append(fs.getUsableSpace() / (1024 * 1024 * 1024)).append(" GB")
                    .append("\n");
        });
        sb.append("Motherboard: ").append(hal.getComputerSystem().getBaseboard().getModel())
                .append(" (Manufacturer: ").append(hal.getComputerSystem().getBaseboard().getManufacturer()).append(")")
                .append("\n");
        sb.append("BIOS: ").append(hal.getComputerSystem().getFirmware().getManufacturer())
                .append(" Version: ").append(hal.getComputerSystem().getFirmware().getVersion())
                .append("\n");
        sb.append("Java Version: ").append(System.getProperty("java.version")).append("\n");
        sb.append("Java Vendor: ").append(System.getProperty("java.vendor")).append("\n");

        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(getMachineInfo());
    }

}
