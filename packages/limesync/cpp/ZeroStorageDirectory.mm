#import <Foundation/Foundation.h>

#include "ZeroStorageDirectory.hpp"
#include <stdexcept>

namespace margelo::nitro::limesync {

std::string zeroStorageDirectory() {
  @autoreleasepool {
    NSFileManager *files = NSFileManager.defaultManager;
    NSError *error = nil;
    NSURL *support = [files URLForDirectory:NSApplicationSupportDirectory
                                    inDomain:NSUserDomainMask
                           appropriateForURL:nil
                                      create:YES
                                       error:&error];
    NSURL *parent = [support URLByAppendingPathComponent:@"One" isDirectory:YES];
    NSURL *directory = [parent URLByAppendingPathComponent:@"zero" isDirectory:YES];
    if (support == nil ||
        ![files createDirectoryAtURL:directory
          withIntermediateDirectories:YES attributes:nil error:&error] ||
        ![directory setResourceValue:@YES forKey:NSURLIsExcludedFromBackupKey error:&error]) {
      throw std::runtime_error(std::string("E_ZERO_STORAGE_OPEN: ") +
                               error.localizedDescription.UTF8String);
    }
    return directory.fileSystemRepresentation;
  }
}

}  // namespace margelo::nitro::limesync
