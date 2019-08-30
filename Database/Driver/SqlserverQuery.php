<?php
/**
 * SQL Server driver for DataTables PHP libraries
 *
 *  @author    SpryMedia
 *  @copyright 2013 SpryMedia ( http://sprymedia.co.uk )
 *  @license   http://editor.datatables.net/license DataTables Editor
 *  @link      http://editor.datatables.net
 */

namespace DataTables\Database\Driver;
if (!defined('DATATABLES')) exit();

use PDO;
use DataTables\Database\Query;
use DataTables\Database\Driver\PostgresResult;


/**
 * SQL Server driver for DataTables Database Query class
 *  @internal
 */
class SqlserverQuery extends Query {
	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Private properties
	 */
	private $_stmt;


	protected $_identifier_limiter = array( '[', ']' );

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Public methods
	 */

	static function connect( $user, $pass='', $host='', $port='', $db='', $dsn='' )
	{
		if ( is_array( $user ) ) {
			$opts = $user;
			$user = $opts['user'];
			$pass = $opts['pass'];
			$port = $opts['port'];
			$host = $opts['host'];
			$db   = $opts['db'];
			$dsn  = isset( $opts['dsn'] ) ? $opts['dsn'] : '';
			$pdoAttr = isset( $opts['pdoAttr'] ) ? $opts['pdoAttr'] : array();
		}

		try {
			$pdoAttr[ PDO::ATTR_ERRMODE ] = PDO::ERRMODE_EXCEPTION;

			if ( in_array( 'sqlsrv', PDO::getAvailableDrivers() ) ) {
				// Windows
				if ( $port !== "" ) {
					$port = ",{$port}";
				}

				$pdo = new PDO(
					"sqlsrv:Server={$host}{$port};Database={$db}".self::dsnPostfix( $dsn ),
					$user,
					$pass,
					$pdoAttr
				);
			}
			else {
				// Linux
				if ( $port !== "" ) {
					$port = ":{$port}";
				}

				$pdo = new PDO(
					"dblib:host={$host}{$port};dbname={$db}".self::dsnPostfix( $dsn ),
					$user,
					$pass,
					$pdoAttr
				);
		}

		} catch (\PDOException $e) {
			// If we can't establish a DB connection then we return a DataTables
			// error.
			echo json_encode( array( 
				"error" => "An error occurred while connecting to the database ".
					"'{$db}'. The error reported by the server was: ".$e->getMessage()
			) );
			exit(0);
		}

		return $pdo;
	}



	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Protected methods
	 */

	protected function _prepare( $sql )
	{
		$this->database()->debugInfo( $sql, $this->_bindings );

		$resource = $this->database()->resource();
		$this->_stmt = $resource->prepare( $sql );

		// bind values
		for ( $i=0 ; $i<count($this->_bindings) ; $i++ ) {
			$binding = $this->_bindings[$i];

			$this->_stmt->bindValue(
				$binding['name'],
				$binding['value'],
				$binding['type'] ? $binding['type'] : \PDO::PARAM_STR
			);
		}
	}


	protected function _exec()
	{
		try {
			$this->_stmt->execute();
		}
		catch (PDOException $e) {
			throw new \Exception( "An SQL error occurred: ".$e->getMessage() );
			error_log( "An SQL error occurred: ".$e->getMessage() );
			return false;
		}

		$resource = $this->database()->resource();
		return new SqlserverResult( $resource, $this->_stmt );
	}

  /**
   * Protect field names
   * @param string $identifier String to be protected
   * @return string
   * @internal
   */
  protected function _protect_identifiers($identifier, $field_build = false )
  {
    $idl = $this->_identifier_limiter;

    // No escaping character
    if ( ! $idl ) {
      return $identifier;
    }

    $left = $idl[0];
    $right = $idl[1];

    // Dealing with a function or other expression? Just return immediately
    if (strpos($identifier, '(') !== FALSE || strpos($identifier, '*') !== FALSE || (strpos($identifier, ' ') !== FALSE && !$field_build))
    {
      return $identifier;
    }

    // Going to be operating on the spaces in strings, to simplify the white-space
    $identifier = preg_replace('/[\t ]+/', ' ', $identifier);

    // Find if our identifier has an alias, so we don't escape that
    if ( strpos($identifier, ' as ') !== false ) {
      $alias = strstr($identifier, ' as ');
      $identifier = substr($identifier, 0, - strlen($alias));
    }
    else {
      $alias = '';
    }

    $a = explode('.', $identifier);
    return $left . implode($right.'.'.$left, $a) . $right . $alias;
  }

  /**
   * Create a comma separated field list
   * @param bool $addAlias Flag to add an alias
   * @return string
   * @internal
   */
  protected function _build_field( $addAlias=false )
  {
    $a = array();
    $asAlias = $this->_supportsAsAlias ?
      ' as ' :
      ' ';
    $counter = count($this->_field);
    for ( $i=0 ; $i < $counter ; $i++ ) {
      $field = $this->_field[$i];

      // Keep the name when referring to a table
      if ( $addAlias && $field !== '*' && strpos($field, '(') === false ) {
        $split = preg_split( '/ as (?![^\(]*\))/i', $field );

        if ( count($split) > 1 ) {
          $a[] = $this->_protect_identifiers( $split[0], true ).$asAlias.
            $this->_field_quote. $split[1] .$this->_field_quote;
        }
        else {
          $a[] = $this->_protect_identifiers( $field, true ).$asAlias.
            $this->_field_quote. $field .$this->_field_quote;
        }
      }
      else if ( $addAlias && strpos($field, '(') !== false && ! strpos($field, ' as ') ) {
        $a[] = $this->_protect_identifiers( $field, true ).$asAlias.
          $this->_field_quote. $field .$this->_field_quote;
      }
      else {
        $a[] = $this->_protect_identifiers( $field, true );
      }
    }

    return ' '.implode(', ', $a).' ';
  }

	// SQL Server 2012+ only
	protected function _build_limit()
	{
		$out = '';

		if ( $this->_offset ) {
			$out .= ' OFFSET '.$this->_offset.' ROWS';
		}
		
		if ( $this->_limit ) {
			if ( ! $this->_offset ) {
				$out .= ' OFFSET 0 ROWS';
			}
			$out .= ' FETCH NEXT '.$this->_limit. ' ROWS ONLY';
		}

		return $out;
	}
}

